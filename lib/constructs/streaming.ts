import * as cdk from '@aws-cdk/core';
import * as events from '@aws-cdk/aws-events';
import * as dynamodb from '@aws-cdk/aws-dynamodb';
import * as fs from 'fs';
import * as iam from '@aws-cdk/aws-iam';
import * as lambda from '@aws-cdk/aws-lambda';
import * as kinesis from '@aws-cdk/aws-kinesis';
import * as kinesisanalytics from '@aws-cdk/aws-kinesisanalytics';
import * as kfh from '@aws-cdk/aws-kinesisfirehose';
import * as s3 from '@aws-cdk/aws-s3';
import * as targets from '@aws-cdk/aws-events-targets';
import { envSpecific } from './helpers';

export interface StreamingProps {
  dynamodbTableLive: dynamodb.ITable
  dynamodbTableStatic: dynamodb.ITable
  s3Bucket: s3.IBucket
  streamName: string
}

export class Streaming extends cdk.Construct {
  public readonly kStream: kinesis.Stream;

  constructor(scope: cdk.Construct, id: string, props: StreamingProps) {
    super(scope, id);

    // Create a main datastream
    const kinesisStream = new kinesis.Stream(this, 'stream', {
      streamName: props.streamName,
      shardCount: 1,
      encryption: kinesis.StreamEncryption.MANAGED,
      retentionPeriod: cdk.Duration.days(1),
    });
    this.kStream = kinesisStream;

    // Create an IAM policy for lambda to put data into dynamoDB
    const lambdaToPutRecordInDynamoRole = new iam.Role(this, 'putRecordToDynamo', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    const indexArnLiveTable = `${props.dynamodbTableLive.tableArn}/index/pk-EventTimeStamp-index`;
    const indexArnStaticTable = `${props.dynamodbTableStatic.tableArn}/index/sk-pk-index`;

    lambdaToPutRecordInDynamoRole.addToPolicy(new iam.PolicyStatement({
      resources: [props.dynamodbTableLive.tableArn, indexArnLiveTable,
        props.dynamodbTableStatic.tableArn, indexArnStaticTable],
      actions: ['dynamodb:PutItem', 'dynamodb:UpdateItem', 'dynamodb:Query', 'dynamodb:GetItem', 'dynamodb:BatchWriteItem'],
    }));
    lambdaToPutRecordInDynamoRole.addToPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: ['cloudwatch:PutMetricData'],
    }));
    lambdaToPutRecordInDynamoRole.addToPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: ['ses:SendEmail'],
    }));
    lambdaToPutRecordInDynamoRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    // Create a lambda function for putting telemetry data into DynamoDB
    const putStreamDataInDynamoDBHandler = new lambda.Function(this, 'kinesisStreamDataToDynamo', {
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset('lib/lambda/python/kinesis-to-dynamo/'),
      handler: 'store_stream_to_dynamodb_app.lambda_handler',
      tracing: lambda.Tracing.ACTIVE,
      role: lambdaToPutRecordInDynamoRole,
      timeout: cdk.Duration.seconds(60),
      environment: {
        DYNAMODB_TABLE_NAME_LIVE: props.dynamodbTableLive.tableName,
        DYNAMODB_TABLE_NAME_STATIC: props.dynamodbTableStatic.tableName,
        ENVIRONMENT: envSpecific('', false, false, true),
        CC: 'akshay_chandiramani@discovery.com', // REMOVE FOR PROD
        RECIPIENT: 'uci_tcl@sportscienceagency.com',
        SENDER: 'UCI TCL Personal Best <akshay_chandiramani@discovery.com>',
      },
    });

    // Create an IAM Policy for kinesisAnalytics
    const streamToAnalyticsRole = new iam.Role(this, 'streamToAnalytics', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
    });

    // Kinesis Permissions
    streamToAnalyticsRole.addToPolicy(new iam.PolicyStatement({
      resources: [
        kinesisStream.streamArn,
      ],
      actions: [
        'kinesis:*',
      ],
    }));

    // Lambda Permissions
    streamToAnalyticsRole.addToPolicy(new iam.PolicyStatement({
      resources: [
        putStreamDataInDynamoDBHandler.functionArn,
      ],
      actions: [
        'lambda:InvokeFunction',
        'lambda:GetFunctionConfiguration',
      ],
    }));

    // Kinesis analytics needs to be able to read pre season questionnaire
    props.s3Bucket.grantRead(streamToAnalyticsRole);
    // Create Kinesis analytics and attach the role
    const appSQLcode = fs.readFileSync('./lib/kinesis_analytics/app.sql')
      .toString();
    const appName = envSpecific('process-sensors-data');
    const analytics = new kinesisanalytics.CfnApplication(this, 'analytics', {
      applicationName: appName,
      applicationCode: appSQLcode,
      inputs: [{
        namePrefix: 'SOURCE_SQL_STREAM',
        kinesisStreamsInput: {
          resourceArn: kinesisStream.streamArn,
          roleArn: streamToAnalyticsRole.roleArn,
        },
        inputParallelism: { count: 1 },
        inputSchema: {
          recordFormat: {
            recordFormatType: 'JSON',
            mappingParameters: {
              jsonMappingParameters: {
                recordRowPath: '$',
              },
            },
          },
          recordEncoding: 'UTF-8',
          recordColumns: [
            {
              name: 'InputMessage',
              mapping: '$.Message',
              sqlType: 'VARCHAR(20)',
            },
            {
              name: 'ApiIngestTime',
              mapping: '$.ApiIngestTime',
              sqlType: 'VARCHAR(32)',
            },
            {
              name: 'ServerTimeStamp',
              mapping: '$.TimeStamp',
              sqlType: 'VARCHAR(32)',
            },
            {
              name: 'SeasonID',
              mapping: '$.SeasonID',
              sqlType: 'INTEGER',
            },
            {
              name: 'EventID',
              mapping: '$.EventID',
              sqlType: 'INTEGER',
            },
            {
              name: 'RaceID',
              mapping: '$.RaceID',
              sqlType: 'INTEGER',
            },
            {
              name: 'EventTimeStamp',
              mapping: '$.Captures[0:].TimeStamp',
              sqlType: 'TIMESTAMP',
            },
            {
              name: 'Bib',
              mapping: '$.Captures[0:].Bib',
              sqlType: 'INTEGER',
            },
            {
              name: 'UCIID',
              mapping: '$.Captures[0:].UCIID',
              sqlType: 'BIGINT',
            },
            {
              name: 'RiderRank',
              mapping: '$.Captures[0:].Rank',
              sqlType: 'INTEGER',
            },
            {
              name: 'State',
              mapping: '$.Captures[0:].State',
              sqlType: 'VARCHAR(4)',
            },
            {
              name: 'Distance',
              mapping: '$.Captures[0:].Distance',
              sqlType: 'DECIMAL(1,1)',
            },
            {
              name: 'DistanceProj',
              mapping: '$.Captures[0:].DistanceProj',
              sqlType: 'DECIMAL(1,1)',
            },
            {
              name: 'Speed',
              mapping: '$.Captures[0:].Speed',
              sqlType: 'DECIMAL(4,2)',
            },
            {
              name: 'SpeedMax',
              mapping: '$.Captures[0:].SpeedMax',
              sqlType: 'DECIMAL(4,2)',
            },
            {
              name: 'SpeedAvg',
              mapping: '$.Captures[0:].SpeedAvg',
              sqlType: 'DECIMAL(4,2)',
            },
            {
              name: 'DistanceFirst',
              mapping: '$.Captures[0:].DistanceFirst',
              sqlType: 'DECIMAL(1,1)',
            },
            {
              name: 'DistanceNext',
              mapping: '$.Captures[0:].DistanceNext',
              sqlType: 'DECIMAL(1,1)',
            },
            {
              name: 'Acc',
              mapping: '$.Captures[0:].Acc',
              sqlType: 'DECIMAL(5,4)',
            },
            {
              name: 'Lat',
              mapping: '$.Captures[0:].Pos.Lat',
              sqlType: 'DOUBLE',
            },
            {
              name: 'Lng',
              mapping: '$.Captures[0:].Pos.Lng',
              sqlType: 'DOUBLE',
            },
            {
              name: 'RiderHeartrate',
              mapping: '$.Captures[0:].Heartrate',
              sqlType: 'INTEGER',
            },
            {
              name: 'RiderCadency',
              mapping: '$.Captures[0:].Cadency',
              sqlType: 'INTEGER',
            },
            {
              name: 'RiderPower',
              mapping: '$.Captures[0:].Power',
              sqlType: 'INTEGER',
            },
            {
              name: 'RaceTime',
              mapping: '$.RaceTime',
              sqlType: 'DECIMAL(6,3)',
            },
            {
              name: 'RaceSpeed',
              mapping: '$.RaceSpeed',
              sqlType: 'DECIMAL(6,4)',
            },
            {
              name: 'LapsToGo',
              mapping: '$.LapsToGo',
              sqlType: 'INTEGER',
            },
            {
              name: 'DistanceToGo',
              mapping: '$.DistanceToGo',
              sqlType: 'INTEGER',
            },
            {
              name: 'EliminatedRaceName',
              mapping: '$.RaceName',
              sqlType: 'VARCHAR(50)',
            },
            {
              name: 'EliminatedBib',
              mapping: '$.Bib',
              sqlType: 'INTEGER',
            },
            {
              name: 'EliminatedUCIID',
              mapping: '$.UCIID',
              sqlType: 'BIGINT',
            },
            {
              name: 'EliminatedFirstName',
              mapping: '$.FirstName',
              sqlType: 'VARCHAR(50)',
            },
            {
              name: 'EliminatedLastName',
              mapping: '$.LastName',
              sqlType: 'VARCHAR(50)',
            },
            {
              name: 'EliminatedShortTVName',
              mapping: '$.ShortTVName',
              sqlType: 'VARCHAR(30)',
            },
            {
              name: 'EliminatedTeam',
              mapping: '$.Team',
              sqlType: 'VARCHAR(30)',
            },
            {
              name: 'EliminatedNOC',
              mapping: '$.NOC',
              sqlType: 'VARCHAR(6)',
            },
          ],
        },
      }],
    });
    analytics.node.addDependency(streamToAnalyticsRole);
    const referencePreSeasonQuestionnaire = new kinesisanalytics.CfnApplicationReferenceDataSource(this, 'questionnaire', {
      applicationName: appName,
      referenceDataSource: {
        tableName: 'PRE_SEASON_QUESTIONNAIRE',
        s3ReferenceDataSource: {
          bucketArn: props.s3Bucket.bucketArn,
          referenceRoleArn: streamToAnalyticsRole.roleArn,
          fileKey: 'batch_data/PreSeasonQuestionnaire/current_version.json',
        },
        referenceSchema: {
          recordFormat: {
            recordFormatType: 'JSON',
            mappingParameters: {
              jsonMappingParameters: {
                recordRowPath: '$[0:]',
              },
            },
          },
          recordEncoding: 'UTF-8',
          recordColumns: [
            {
              name: 'FirstName',
              mapping: '$[0:].FirstName',
              sqlType: 'VARCHAR(16)',
            },
            {
              name: 'HeightCm',
              mapping: '$[0:].HeightCm',
              sqlType: 'INTEGER',
            },
            {
              name: 'WeightKg',
              mapping: '$[0:].WeightKg',
              sqlType: 'INTEGER',
            },
            {
              name: 'RestHrBpm',
              mapping: '$[0:].RestHrBpm',
              sqlType: 'INTEGER',
            },
            {
              name: 'MaxHrBpm',
              mapping: '$[0:].MaxHrBpm',
              sqlType: 'INTEGER',
            },
            {
              name: 'Flying200',
              mapping: '$[0:].Flying200',
              sqlType: 'VARCHAR(8)',
            },
            {
              name: 'GearingForFlying200',
              mapping: '$[0:].GearingForFlying200',
              sqlType: 'VARCHAR(4)',
            },
            {
              name: 'PowerPeakW',
              mapping: '$[0:].PowerPeakW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Power5sW',
              mapping: '$[0:].Power5sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Power15sW',
              mapping: '$[0:].Power15sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Power30sW',
              mapping: '$[0:].Power30sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'LastName',
              mapping: '$[0:].LastName',
              sqlType: 'VARCHAR(4)',
            },
            {
              name: 'Power60sW',
              mapping: '$[0:].Power60sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Power120sW',
              mapping: '$[0:].Power120sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Power180sW',
              mapping: '$[0:].Power180sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Power300sW',
              mapping: '$[0:].Power300sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Power600sW',
              mapping: '$[0:].Power600sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Power1200sW',
              mapping: '$[0:].Power1200sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Power1800sW',
              mapping: '$[0:].Power1800sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Power3600sW',
              mapping: '$[0:].Power3600sW',
              sqlType: 'INTEGER',
            },
            {
              name: 'Bip',
              mapping: '$[0:].Bip',
              sqlType: 'VARCHAR(16)',
            },
            {
              name: 'BirthDate',
              mapping: '$[0:].BirthDate',
              sqlType: 'VARCHAR(16)',
            },
            {
              name: 'UCIID',
              mapping: '$[0:].UCIID',
              sqlType: 'BIGINT',
            },
            {
              name: 'Gender',
              mapping: '$[0:].Gender',
              sqlType: 'VARCHAR(4)',
            },
            {
              name: 'TrainingLocation',
              mapping: '$[0:].TrainingLocation',
              sqlType: 'VARCHAR(32)',
            },
            {
              name: 'LeagueCat',
              mapping: '$[0:].LeagueCat',
              sqlType: 'VARCHAR(16)',
            },
            {
              name: 'Nationality',
              mapping: '$[0:].Nationality',
              sqlType: 'VARCHAR(4)',
            },
            {
              name: 'SeasonTitle',
              mapping: '$[0:].SeasonTitle',
              sqlType: 'VARCHAR(32)',
            },
          ],
        },
      },
    });
    referencePreSeasonQuestionnaire.node.addDependency(streamToAnalyticsRole);
    referencePreSeasonQuestionnaire.addDependsOn(analytics);

    // Create output for analytics
    const liveRidersTrackingStreamOutput = new kinesisanalytics.CfnApplicationOutput(this,
      'KinesisOutputRawTelemetryData', {
        applicationName: appName,
        output: {
          name: 'TELEMETRY_RAW_DATA_ENRICHED',
          lambdaOutput: {
            resourceArn: putStreamDataInDynamoDBHandler.functionArn,
            roleArn: streamToAnalyticsRole.roleArn,
          },
          destinationSchema: {
            recordFormatType: 'JSON',
          },
        },
      });
    liveRidersTrackingStreamOutput.node.addDependency(analytics);

    const otherDataOutput = new kinesisanalytics.CfnApplicationOutput(this, 'KinesisOutputOtherData', {
      applicationName: appName,
      output: {
        name: 'OTHER_LIVE_DATA',
        lambdaOutput: {
          resourceArn: putStreamDataInDynamoDBHandler.functionArn,
          roleArn: streamToAnalyticsRole.roleArn,
        },
        destinationSchema: {
          recordFormatType: 'JSON',
        },
      },
    });
    otherDataOutput.node.addDependency(analytics);

    const aggregatesDataOutput = new kinesisanalytics.CfnApplicationOutput(this, 'KinesisAggregatesData', {
      applicationName: appName,
      output: {
        name: 'AGGREGATES',
        lambdaOutput: {
          resourceArn: putStreamDataInDynamoDBHandler.functionArn,
          roleArn: streamToAnalyticsRole.roleArn,
        },
        destinationSchema: {
          recordFormatType: 'JSON',
        },
      },
    });
    aggregatesDataOutput.node.addDependency(analytics);

    // Create a role for firehose and assign it to kinesis stream
    const firehoseRole = new iam.Role(this, 'firehoseToS3', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });
    kinesisStream.grantRead(firehoseRole);
    firehoseRole.addToPolicy(new iam.PolicyStatement({
      resources: [kinesisStream.streamArn],
      actions: ['kinesis:DescribeStream'],
    }));

    // Grant write permission to firehoseRole to wright to the RAW S3 bucket
    props.s3Bucket.grantReadWrite(firehoseRole);

    // Create a FireHose and add dependency to the Stream and the firehoseRole
    const firehose = new kfh.CfnDeliveryStream(this, 'firehose', {
      //  deliveryStreamName: 'FirehoseDeliveryStream',
      deliveryStreamType: 'KinesisStreamAsSource',
      kinesisStreamSourceConfiguration: {
        kinesisStreamArn: kinesisStream.streamArn,
        roleArn: firehoseRole.roleArn,
      },
      extendedS3DestinationConfiguration: {
        bucketArn: props.s3Bucket.bucketArn,
        bufferingHints: {
          intervalInSeconds: 60,
          sizeInMBs: 5,
        },
        roleArn: firehoseRole.roleArn,
        prefix: 'stream_data/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/',
        errorOutputPrefix: 'stream_data_errors',
      },
    });
    firehose.node.addDependency(kinesisStream);
    firehose.node.addDependency(firehoseRole);

    // Create a role for a lambda function for Kinesis Analytics launch and restart
    const lambdaKDS = new iam.Role(this, 'maintainKDSrole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });
    lambdaKDS.addToPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: [
        'kinesisanalytics:GetApplicationState',
        'kinesisanalytics:DescribeApplication',
        'kinesisanalytics:ListApplications',
        'kinesisanalytics:StartApplication',
        'kinesisanalytics:StopApplication',
      ],
    }));
    lambdaKDS.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    // Create a lambda function for for Kinesis Analytics launch and restart
    const maintainKDS = new lambda.Function(this, 'maintainKDS', {
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset('lib/lambda/python/maintainkds/'),
      description: 'This lambda checks the Kinesis Analytics service every 5 mins and restarts it if needed.',
      handler: 'maintain.lambda_handler',
      tracing: lambda.Tracing.ACTIVE,
      role: lambdaKDS,
      timeout: cdk.Duration.seconds(60),
      environment: {
        ENABLED: 'true',
        RESTART: 'true',
        RESTARTTIME: '02:30:00',
        APP: appName,
      },
    });

    // Create event rule to trigger the Lambda every 5 minutes
    const eventRule = new events.Rule(this, 'fiveMinuteRule', {
      schedule: events.Schedule.cron({ minute: '0/5' }),
    });
    eventRule.addTarget(
      new targets.LambdaFunction(maintainKDS, {
        event: events.RuleTargetInput.fromEventPath('$.time'),
      }),
    );
  }
}
