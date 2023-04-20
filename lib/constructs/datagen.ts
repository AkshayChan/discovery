import * as cdk from '@aws-cdk/core';
import * as lambda from '@aws-cdk/aws-lambda';
import {
  Role, ManagedPolicy, ServicePrincipal,
} from '@aws-cdk/aws-iam';
import * as apigateway from '@aws-cdk/aws-apigateway';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import { MethodLoggingLevel } from '@aws-cdk/aws-apigateway';
import { envSpecific } from './helpers';
import * as iam from "@aws-cdk/aws-iam";

export interface DataGenerationProps {
    readonly apiURL: string;
    readonly inApiKeySecret: secretsmanager.Secret;
}

export class DataGeneration extends cdk.Construct {
  constructor(scope: cdk.Construct, id: string, props: DataGenerationProps) {
    super(scope, id);

    // Create a basic service role for the Lambda function, and give it access to Kinesis
    const LambdaServiceRole = new Role(this, 'DataGeneratorLambdaServiceRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
      ],
    });

    LambdaServiceRole.addToPolicy(new iam.PolicyStatement({
      resources: [
        props.inApiKeySecret.secretArn
      ],
      actions: [
        'secretsmanager:GetResourcePolicy',
        'secretsmanager:GetSecretValue',
        'secretsmanager:DescribeSecret',
        'secretsmanager:ListSecretVersionIds'
      ],
    }));

    const dataGenLambda = new lambda.Function(this, 'TrackRacingSimulator', {
      code: lambda.Code.fromAsset('lib/lambda/python/data_generator'),
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'data_generator.lambda_handler',
      role: LambdaServiceRole,
      timeout: cdk.Duration.seconds(600),
      environment: {
        RACES_COUNT: '2',
        RACER_COUNT: '2',
        RACE_DURATION_SEC: '60',
        API_URL: props.apiURL ?? '',
        SECRET_MANAGER_NAME_FOR_API_KEY: props.inApiKeySecret.secretName ?? '',
      },
    });

    const dataGenApi = new apigateway.RestApi(this, envSpecific('DataGenAPI'), {
      deployOptions: {
        stageName: process.env.DEPLOY_ENV || 'dev',
        tracingEnabled: true,
        loggingLevel: MethodLoggingLevel.INFO,
      },
    });

    const restApiLambdaIntegration = new apigateway.LambdaIntegration(dataGenLambda);

    const dataGenResource = dataGenApi.root.addResource('dataGen');

    dataGenResource.addMethod('GET', restApiLambdaIntegration, {
      apiKeyRequired: false,
    });
  }
}
