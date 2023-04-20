import * as apigateway from '@aws-cdk/aws-apigateway';
import { MethodLoggingLevel, Model } from '@aws-cdk/aws-apigateway';
import * as cdk from '@aws-cdk/core';
import * as ddb from '@aws-cdk/aws-dynamodb';
import * as lambda from '@aws-cdk/aws-lambda';
import * as s3 from '@aws-cdk/aws-s3';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import { ManagedPolicy, Role, ServicePrincipal } from '@aws-cdk/aws-iam';
import * as s3deploy from '@aws-cdk/aws-s3-deployment';
import { envSpecific } from './helpers';
import * as ssm from '@aws-cdk/aws-ssm';

export interface RestAPIprops {
    ddbTableStatic: ddb.ITable;
    ddbTableLive: ddb.ITable;
    kinesisStream: string;
    s3raw: s3.IBucket;
    s3Swagger: s3.IBucket;
}

export class RestAPI extends cdk.Construct {
    public readonly api: apigateway.RestApi;

    public readonly RestAPIGwSecretProducer: secretsmanager.Secret;

    public readonly RestAPIGwSecretConsumer: secretsmanager.Secret;

    constructor(scope: cdk.Construct, id: string, props: RestAPIprops) {
      super(scope, id);

      this.api = new apigateway.RestApi(this, envSpecific('RestAPIGwConsumer'), {
        deployOptions: {
          stageName: process.env.DEPLOY_ENV || 'dev',
          tracingEnabled: true,
          loggingLevel: MethodLoggingLevel.INFO,
        },
        description: 'Rest API for TCL UCI',
      });

      // ####################################
      // ########## KINESIS FLOW ############
      // ####################################
      // Create a basic service role for the Lambda function, and give it access to Kinesis
      const LambdaServiceRole = new Role(this, 'LambdaServiceRole', {
        assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
        managedPolicies: [
          // !! We need to modify this permissions after!
          ManagedPolicy.fromAwsManagedPolicyName('AmazonKinesisFullAccess'),
          ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
          ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
          ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
        ],
      });

      // Create lambda function for data ingestion into Kinesis or S3
      const KinesisLambdaFunction = new lambda.Function(this, 'KinesisAPILambda', {
        code: lambda.Code.fromAsset('lib/lambda/python/kinesis-s3-api'),
        runtime: lambda.Runtime.PYTHON_3_8,
        handler: 'main.handler',
        role: LambdaServiceRole,
        timeout: cdk.Duration.seconds(30),
        environment: {
          KINESIS_SINK: props.kinesisStream,
          DYNAMODB_TABLE: props.ddbTableStatic.tableName,
          OUTPUT_S3_BUCKET: props.s3raw.bucketName,
          OUTPUT_S3_KEY: 'batch_data/',
        },
        tracing: lambda.Tracing.ACTIVE,
      });
      props.ddbTableLive.grantReadWriteData(KinesisLambdaFunction);
      props.ddbTableStatic.grantReadWriteData(KinesisLambdaFunction);
      const kinesisLambdaIntegration = new apigateway.LambdaIntegration(KinesisLambdaFunction, {
        proxy: true,
      });

      // Generate Secret for RestAPI Producer (like data generator)
      this.RestAPIGwSecretProducer = new secretsmanager.Secret(this, 'RestAPIGwInKey', {
        generateSecretString: {
          generateStringKey: 'api_key',
          secretStringTemplate: JSON.stringify({ username: 'api_user' }),
          excludeCharacters: ' %+~`#$&*()|[]{}:;<>?!\'/@"\\',
        },
      });

      new ssm.StringParameter(this, 'SSMApiURL', {
        stringValue: this.api.url,
        parameterName: envSpecific('/ucitcl/restapi/url',false,true),
      });

      new ssm.StringParameter(this, 'SSMApiKey', {
        stringValue: this.RestAPIGwSecretProducer.secretValueFromJson('api_key').toString(),
        parameterName: envSpecific('/ucitcl/restapi/key',false,true),
      });

      // RestAPI post configuration
      const StoreRacesList = this.api.root.addResource('StoreRacesList');
      StoreRacesList.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StoreRaceStartList = this.api.root.addResource('StoreRaceStartList');
      StoreRaceStartList.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StoreRaceResults = this.api.root.addResource('StoreRaceResults');
      StoreRaceResults.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StoreLiveRidersTracking = this.api.root.addResource('StoreLiveRidersTracking');
      StoreLiveRidersTracking.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StoreLiveRidersData = this.api.root.addResource('StoreLiveRidersData');
      StoreLiveRidersData.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StorePreSeasonQuestionnaire = this.api.root.addResource('StorePreSeasonQuestionnaire');
      StorePreSeasonQuestionnaire.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StoreStartTime = this.api.root.addResource('StoreStartTime');
      StoreStartTime.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StoreLapCounter = this.api.root.addResource('StoreLapCounter');
      StoreLapCounter.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StoreRiderEliminated = this.api.root.addResource('StoreRiderEliminated');
      StoreRiderEliminated.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StoreFinishTime = this.api.root.addResource('StoreFinishTime');
      StoreFinishTime.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StoreClassification = this.api.root.addResource('StoreClassification');
      StoreClassification.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      const StoreRaceStartLive = this.api.root.addResource('StoreRaceStartLive');
      StoreRaceStartLive.addMethod('POST', kinesisLambdaIntegration, {
        apiKeyRequired: true,
      });

      // Add a custom API key
      const keyIn = this.api.addApiKey('In', {
        value: this.RestAPIGwSecretProducer.secretValueFromJson('api_key').toString(),
      });

      // Add the API usage plan`
      const plan = this.api.addUsagePlan('KinesisAPIUsagePlan');

      // Wait till the key is there and after make a deployment
      plan.node.addDependency(keyIn);
      plan.addApiKey(keyIn);
      plan.addApiStage({
        stage: this.api.deploymentStage,
      });

      // #####################################
      // ########## REST API FLOW ############
      // #####################################

      // Create a lambda function for DynamoDB interruction.
      const RestApiDdbHandler = new lambda.Function(this, 'RestApiHandler', {
        runtime: lambda.Runtime.NODEJS_12_X,
        code: lambda.Code.fromAsset('lib/lambda/node/api/dist'),
        handler: 'index.handler',
        tracing: lambda.Tracing.ACTIVE,
        timeout: cdk.Duration.seconds(30),
        environment: {
          DDB_TABLE_STATIC: props.ddbTableStatic.tableName,
          DDB_TABLE_LIVE: props.ddbTableLive.tableName,
        },
      });
      props.ddbTableStatic.grantReadWriteData(RestApiDdbHandler);
      props.ddbTableLive.grantReadWriteData(RestApiDdbHandler);

      // Assign Lambda to the RestAPIgwConsumer
      const restApiLambdaIntegration = new apigateway.LambdaIntegration(RestApiDdbHandler, {
        proxy: true,
      });

      const resourceV1 = this.api.root.addResource('v1');
      const proxy = resourceV1.addResource('{proxy+}');

      proxy.addMethod('ANY', restApiLambdaIntegration, {
        apiKeyRequired: true,
      });

      // Generate Secret for RestAPI Consumer
      this.RestAPIGwSecretConsumer = new secretsmanager.Secret(this, 'RestApiConsumerKey', {
        generateSecretString: {
          generateStringKey: 'api_key',
          secretStringTemplate: JSON.stringify({ username: 'api_user' }),
          excludeCharacters: ' %+~`#$&*()|[]{}:;<>?!\'/@"\\',
        },
      });

      // Add a custom API Key
      const keyConsumer = this.api.addApiKey('Consumer', {
        value: this.RestAPIGwSecretConsumer.secretValueFromJson('api_key').toString(),
      });

      // Create a plan
      const usagePlan = this.api.addUsagePlan('ConsumerAPIUsagePlan');

      // Wait for key readiness and make an attachment
      usagePlan.node.addDependency(keyConsumer);
      usagePlan.addApiKey(keyConsumer);
      usagePlan.addApiStage({
        stage: this.api.deploymentStage,
      });

      const resourceDocs = this.api.root.addResource('docs');
      const item = resourceDocs.addResource('api-doc.yml');

      const s3Role = new Role(this, 's3Role', {
        assumedBy: new ServicePrincipal('apigateway.amazonaws.com'),
        managedPolicies: [
          // !! We need to modify this permissions after!  ONLY ALLOW ACCES TO Swagger bucket
          ManagedPolicy.fromAwsManagedPolicyName('AmazonS3ReadOnlyAccess'),
        ],

      });

      const integrationRequest = new apigateway.AwsIntegration({
        service: 's3',
        path: `/${props.s3Swagger.bucketName}/index.html`,
        options: {
          credentialsRole: s3Role,
          integrationResponses: [
            { statusCode: '200' },
          ],
        },
        integrationHttpMethod: 'GET',
      });

      resourceDocs.addMethod('GET', integrationRequest, {
        apiKeyRequired: false,
        methodResponses: [
          {
            statusCode: '200',
            responseModels: {
              'text/html': Model.EMPTY_MODEL,
            },
          },
        ],
      });

      const itemIntegrationRequest = new apigateway.AwsIntegration({
        service: 's3',
        path: `/${props.s3Swagger.bucketName}/api-doc.yml`,
        options: {
          credentialsRole: s3Role,
          integrationResponses: [
            { statusCode: '200' },
          ],

        },
        integrationHttpMethod: 'GET',
      });

      item.addMethod('GET', itemIntegrationRequest, {
        apiKeyRequired: false,
        methodResponses: [
          {
            statusCode: '200',
            responseModels: {
              'text/yaml': Model.EMPTY_MODEL,
            },
          },
        ],
      });

      /// Upload the latest API docs
      new s3deploy.BucketDeployment(this, 'copySwaggerDocs', {
        sources: [s3deploy.Source.asset('./lib/lambda/node/api/docs')],
        destinationBucket: props.s3Swagger,
      });
    }
}
