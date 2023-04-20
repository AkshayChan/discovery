import * as cdk from '@aws-cdk/core';
import * as cloudfront from '@aws-cdk/aws-cloudfront';
import * as cognito from '@aws-cdk/aws-cognito';
import * as iam from '@aws-cdk/aws-iam';
import * as s3 from '@aws-cdk/aws-s3';
import * as s3deploy from '@aws-cdk/aws-s3-deployment';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';

export interface EurosportCyclingFrontendProps extends cdk.StackProps {
    readonly bucket: s3.Bucket;
    readonly distribution: cloudfront.CloudFrontWebDistribution;

}

export class EurosportCyclingFrontend extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props: EurosportCyclingFrontendProps) {
    super(scope, id, props);
    // Upload content to the S3
    new s3deploy.BucketDeployment(this, 'UploadBuildFolder', {
      sources: [s3deploy.Source.asset('./operations_dashboard/build')],
      destinationBucket: props.bucket,
      distribution: props.distribution,
      distributionPaths: ['/*'],
    });
  }
}
