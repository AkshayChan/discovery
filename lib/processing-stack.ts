import * as cdk from '@aws-cdk/core';
import * as dynamodb from '@aws-cdk/aws-dynamodb';
import * as kinesis from '@aws-cdk/aws-kinesis';
import * as s3 from '@aws-cdk/aws-s3';
import { Glue } from './constructs/glue';
import { VerifySesEmailAddress } from './constructs/verify-ses-email';
import { Streaming } from './constructs/streaming';
import { DataGeneration } from './constructs/datagen';

export interface ProcessingStackProps extends cdk.StackProps {
    readonly ddbTableStatic: dynamodb.ITable;
    readonly ddbTableLive: dynamodb.ITable;
    readonly s3raw: s3.IBucket;
    readonly s3staging: s3.IBucket;
    readonly s3poststaging: s3.IBucket;
    readonly s3Temp: s3.IBucket;
    readonly streamName: string;
}

export class EurosportCyclingProcessing extends cdk.Stack {
  public readonly kinesisStream: kinesis.Stream;

  public readonly apiURL: string;

  public readonly inApiKeySecret: string;

  public readonly outApiKey: string

  constructor(scope: cdk.Construct, id: string, props: ProcessingStackProps) {
    super(scope, id, props);

    // Deploy glue setup. Tabels databases and jobs
    new Glue(this, 'Glue', {
      s3raw: props.s3raw,
      s3staging: props.s3staging,
      s3poststaging: props.s3poststaging,
      s3Temp: props.s3Temp,
      ddbTableStatic: props.ddbTableStatic,
    });

    // Verify SES sender email address
    new VerifySesEmailAddress(this, 'SesEmailVerificationSender', {
      emailAddress: 'akshay_chandiramani@discovery.com',
      region: cdk.Stack.of(this).region,
    });

    // Verify SES recipient email address
    new VerifySesEmailAddress(this, 'SesEmailVerificationRecipient', {
      emailAddress: 'uci_tcl@sportscienceagency.com',
      region: cdk.Stack.of(this).region,
    });

    // Deploy kinessis stream stup.
    const streamingApp = new Streaming(this, 'Sensors', {
      dynamodbTableLive: props.ddbTableLive,
      dynamodbTableStatic: props.ddbTableStatic,
      s3Bucket: props.s3raw,
      streamName: props.streamName,
    });
    this.kinesisStream = streamingApp.kStream;
  }
}
