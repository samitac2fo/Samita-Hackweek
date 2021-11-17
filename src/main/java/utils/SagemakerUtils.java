package utils;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sagemaker.SageMakerClient;
import software.amazon.awssdk.services.sagemaker.model.*;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.*;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.FeatureValue;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.GetRecordRequest;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.GetRecordResponse;

import java.util.ArrayList;
import java.util.List;

// https://aws.amazon.com/blogs/machine-learning/use-amazon-sagemaker-feature-store-in-a-java-environment/
public class SagemakerUtils {
    public static SageMakerClient sageMakerClient;
    public static S3Client s3Client;
    public static SageMakerFeatureStoreRuntimeClient sageMakerFeatureStoreRuntimeClient;
    public static OfflineStoreConfig offlineStoreConfig;
    public static OnlineStoreConfig onlineStoreConfig;
    public static S3StorageConfig s3StorageConfig;

    static final String BUCKET_NAME = "sagemaker-us-west-2-865009241861";
    private Region region;
    private static String roleArn = "arn:aws:iam::865009241861:role/service-role/AmazonSageMaker-ExecutionRole-20190123T104508";
    private static int numOfThreadsToCreate = 4;

    public SagemakerUtils(Region region) {
        this.region = region;

        s3StorageConfig = S3StorageConfig.builder()
            .s3Uri(String.format("s3://%1$s/sagemaker-featurestore", BUCKET_NAME)).build();

        offlineStoreConfig = OfflineStoreConfig.builder()
            .s3StorageConfig(s3StorageConfig).build();

        onlineStoreConfig = OnlineStoreConfig.builder()
            .enableOnlineStore(Boolean.TRUE).build();

        sageMakerClient = SageMakerClient.builder()
            .region(this.region).build();

        s3Client = S3Client.builder()
            .region(this.region).build();

        sageMakerFeatureStoreRuntimeClient = SageMakerFeatureStoreRuntimeClient.builder()
            .region(this.region).build();
    }

    public static void main(String args[]){
        // feature names
        String[] featureGroupNames = {"test-hackweek-samita"};
        String[] featureNames = {"TransactionID", "EventTime"};

        // Initialize Client
        SagemakerUtils sagemakerUtils = new SagemakerUtils(Region.US_WEST_2);

        // create dummy data
        List<String[]> data = new ArrayList<>();
        String[] dummy1 = {"123", "2021-09-15T19:56:25.000Z"};
        data.add(dummy1);

        // Build and create a list of records
        List<List<FeatureValue>> featureRecordsList = FeatureGroupRecordOperations.makeRecordsList(featureNames, data, false, false);

        // Create Feature Group
        String featureGroupResponse =
            sagemakerUtils.createFeatureGroup(featureGroupNames[0], "hackweek-samita", "TransactionID", "EventTime", true);
        System.out.println(String.format("Feature Group: %s", featureGroupResponse));

        // Describe Feature Group]\
        sagemakerUtils.describeFeatureGroup(featureGroupNames[0]);

        // Ingest Data
        sagemakerUtils.ingestData(featureGroupNames, featureRecordsList, 1);

        // Get Feature group instance - custom usage
        DescribeFeatureGroupResponse getFeatureGroup = sagemakerUtils.getFeatureGroup(featureGroupNames[0]);
        System.out.println(String.format("Feature Group: %s", getFeatureGroup.toString()));

        // Get Feature store records - Online Store Only

        // Get Feature store records - Offline Store

        // Delete Feature group
        DeleteFeatureGroupResponse deleteFeatureGroupResponse = sagemakerUtils.deleteFeatureGroup(featureGroupNames[0]);
        System.out.println(String.format("Feature Group Deleted: %s", deleteFeatureGroupResponse.toString()));

        sageMakerFeatureStoreRuntimeClient.close();
        sageMakerClient.close();
        s3Client.close();
    }

    public void ingestData(final String[] featureGroupNames, final List<List<FeatureValue>> featureRecordsList, final Integer numOfThreadsToCreate) {
        Ingest.batchIngest(numOfThreadsToCreate, sageMakerFeatureStoreRuntimeClient, featureRecordsList, featureGroupNames, "EventTime");
    }

    public void describeFeatureGroup(final String featureGroupName) {
        DescribeFeatureGroupResponse describeResponse = this.getFeatureGroup(featureGroupName);
        System.out.println("\nFeature group name is: " + describeResponse.featureGroupName());
        System.out.println("\nFeature group creation time is: " + describeResponse.creationTime());
        System.out.println("\nFeature group feature Definitions is: " + describeResponse.featureDefinitions());
        System.out.println("\nFeature group feature Role Arn is: " + describeResponse.roleArn());
        System.out.println("\nFeature group description is: " + describeResponse.description());
    }

    public DeleteFeatureGroupResponse deleteFeatureGroup(final String featureGroupName){
        DeleteFeatureGroupRequest deleteFeatureGroupRequest =
            DeleteFeatureGroupRequest.builder()
                .featureGroupName(featureGroupName)
                .build();
        return sageMakerClient.deleteFeatureGroup(deleteFeatureGroupRequest);
    }

    public DescribeFeatureGroupResponse getFeatureGroup(final String featureGroupName) {
        DescribeFeatureGroupRequest describeFeatureGroupRequest =
            DescribeFeatureGroupRequest.builder()
                .featureGroupName(featureGroupName)
                .build();
        return sageMakerClient.describeFeatureGroup(describeFeatureGroupRequest);
    }

    public String createFeatureGroup(final String featureGroupName, final String description,
         final String recordIdFeatureName, final String eventTimeFeatureName, final Boolean enableOnlineStore) {
        String featureGroupArn = null;

        List<FeatureDefinition> featureDefinitionList = new ArrayList<>();
        featureDefinitionList.add(FeatureDefinition.builder().featureName("TransactionID").featureType(FeatureType.STRING).build());
        featureDefinitionList.add(FeatureDefinition.builder().featureName("EventTime").featureType(FeatureType.STRING).build());

        OnlineStoreConfig onlineStoreConfig =
            OnlineStoreConfig.builder().enableOnlineStore(enableOnlineStore).build();

        CreateFeatureGroupRequest createFeatureGroupRequest =
            CreateFeatureGroupRequest.builder()
                .featureDefinitions(featureDefinitionList)
                .featureGroupName(featureGroupName)
                .description(description)
                .recordIdentifierFeatureName(recordIdFeatureName)
                .eventTimeFeatureName(eventTimeFeatureName)
                .onlineStoreConfig(onlineStoreConfig)
                .roleArn(roleArn)
                .build();

        try {
            featureGroupArn = sageMakerClient.createFeatureGroup(createFeatureGroupRequest).featureGroupArn();
        } catch (ResourceInUseException resourceInUseException){
            System.err.println("Resource Group could not be created:" + resourceInUseException);
        }
        return featureGroupArn == null ? "Duplicate Resource Group" + this.getFeatureGroup(featureGroupName).featureGroupArn(): featureGroupArn;
    }
}