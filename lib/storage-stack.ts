import * as cdk from '@aws-cdk/core';
import * as dynamodb from '@aws-cdk/aws-dynamodb';
import * as s3 from '@aws-cdk/aws-s3';
import * as s3deploy from '@aws-cdk/aws-s3-deployment';

export interface EurosportCyclingStorageProps extends cdk.StackProps {}

export class EurosportCyclingStorage extends cdk.Stack {
    public readonly ddbTableStatic: dynamodb.Table;

    public readonly ddbTableLive: dynamodb.Table;

    public readonly bucketRaw: s3.Bucket;

    public readonly bucketStaging: s3.Bucket;

    public readonly bucketPostStaging: s3.Bucket;

    public readonly bucketTemp: s3.Bucket;

    public readonly bucketSwagger: s3.Bucket;

    constructor(scope: cdk.Construct, id: string, props: EurosportCyclingStorageProps) {
      super(scope, id, props);
      // Create DynamoDB table for static data
      this.ddbTableStatic = new dynamodb.Table(this, 'static', {
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        partitionKey: {
          name: 'pk',
          type: dynamodb.AttributeType.STRING,
        },
        sortKey: {
          name: 'sk',
          type: dynamodb.AttributeType.STRING,
        },
        // stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      this.ddbTableStatic.addGlobalSecondaryIndex({
        partitionKey: {
          name: 'sk',
          type: dynamodb.AttributeType.STRING,
        },
        sortKey: {
          name: 'pk',
          type: dynamodb.AttributeType.STRING,
        },
        indexName: 'sk-pk-index',
      });

      // Create DynamoDB table for live data
      this.ddbTableLive = new dynamodb.Table(this, 'live', {
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        partitionKey: {
          name: 'pk',
          type: dynamodb.AttributeType.STRING,
        },
        sortKey: {
          name: 'sk',
          type: dynamodb.AttributeType.STRING,
        },
        stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      this.ddbTableLive.addGlobalSecondaryIndex({
        partitionKey: {
          name: 'pk',
          type: dynamodb.AttributeType.STRING,
        },
        sortKey: {
          name: 'EventTimeStamp',
          type: dynamodb.AttributeType.STRING,
        },
        indexName: 'pk-EventTimeStamp-index',
      });
      // Create S3 buckets.
      // This buckets are note removed after stack destroy
      const s3Swagger = new s3.Bucket(this, 'swagger', {
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      });
      this.bucketSwagger = s3Swagger;

      const s3raw = new s3.Bucket(this, 'raw', {
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      });
      this.bucketRaw = s3raw;

      const s3staging = new s3.Bucket(this, 'staging', {
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      });
      this.bucketStaging = s3staging;

      const s3postStaging = new s3.Bucket(this, 'poststaging', {
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      });
      this.bucketPostStaging = s3postStaging;

      // Create a service bucket for temporary store components.
      // ! This bucket has a removal policy on! It will be removed on stack destroy
      const s3temp = new s3.Bucket(this, 'temp', {
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        autoDeleteObjects: true,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      });
      this.bucketTemp = s3temp;

      // Upload folder to the bucket
      new s3deploy.BucketDeployment(this, 'uploadGlueJobsFolder', {
        sources: [s3deploy.Source.asset('./glue_jobs')],
        destinationBucket: s3temp,
        cacheControl: [s3deploy.CacheControl.fromString('max-age=0,no-cache,no-store,must-revalidate')],
        destinationKeyPrefix: 'glue_jobs',
      });
    }
}
