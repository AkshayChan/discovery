import * as cdk from '@aws-cdk/core';
import {
  CfnOutput, ConcreteDependable, Duration, Expiration,
} from '@aws-cdk/core';
import * as appsync from '@aws-cdk/aws-appsync';
import {
  AppsyncFunction,
  BaseDataSource, CfnApiCache, CfnResolver,
  DynamoDbDataSource,
  GraphqlApi,
  MappingTemplate,
  Resolver,
} from '@aws-cdk/aws-appsync';
import * as ddb from '@aws-cdk/aws-dynamodb';
import * as iam from '@aws-cdk/aws-iam';
import * as fs from 'fs';
import * as path from 'path';
import * as lambda from '@aws-cdk/aws-lambda';
import { Tracing } from '@aws-cdk/aws-lambda';
import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks';
import { envSpecific, hashCode } from './helpers';

const resolverConfig = require('../../graphql/config.json');

type DataSourceResolver = {
    name: string;
    type: string;
    response: string;
    request: string;
    functions: [string];
};

type DataSource = {
    Mutation: [DataSourceResolver];
    Query: [DataSourceResolver];
    functions: [DataSourceResolver];
};

export interface AppSyncProps {
    readonly eurosportCyclingStaticDdb: ddb.ITable;
    readonly eurosportCyclingLiveDdb: ddb.ITable;
}

export class AppSync extends cdk.Construct {
    public readonly urlOutput: CfnOutput;

    resolverDeps: ConcreteDependable;

    datasources: { [key: string]: BaseDataSource | DynamoDbDataSource };

    eurosportCyclingApi: GraphqlApi;

    cache: CfnApiCache;

    appsyncFunctions: { [key: string]: AppsyncFunction } = {};

    ctx: AppSyncProps;

    constructor(scope: cdk.Construct, id: string, props: AppSyncProps) {
      super(scope, id);
      this.ctx = props;

      this.resolverDeps = new ConcreteDependable();

      const apiLogsRole = new iam.Role(this, 'appsyncLoggingRole', {
        assumedBy: new iam.ServicePrincipal('appsync.amazonaws.com'),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            'service-role/AWSAppSyncPushToCloudWatchLogs',
          ),
        ],
      });

      this.eurosportCyclingApi = new appsync.GraphqlApi(this, 'Api', {
        name: envSpecific('eurosport-cycling'),
        schema: appsync.Schema.fromAsset('graphql/schema.graphql'),
        authorizationConfig: {
          defaultAuthorization: {
            authorizationType: appsync.AuthorizationType.API_KEY,
            apiKeyConfig: {
              expires: Expiration.after(Duration.days(365)),
            },
          },
        },
        xrayEnabled: true,
        logConfig: {
          fieldLogLevel: appsync.FieldLogLevel.ALL,
          role: apiLogsRole,
        },
      });

      this.cache = new CfnApiCache(this, 'ResolverCache', {
        type: 'SMALL',
        apiId: this.eurosportCyclingApi.apiId,
        ttl: 360,
        atRestEncryptionEnabled: true,
        transitEncryptionEnabled: true,
        apiCachingBehavior: 'PER_RESOLVER_CACHING',
      });

      const ddbStaticDs = this.eurosportCyclingApi.addDynamoDbDataSource('ddbStaticSource', props.eurosportCyclingStaticDdb);
      const ddbLiveDs = this.eurosportCyclingApi.addDynamoDbDataSource('ddbLiveSource', props.eurosportCyclingLiveDdb);

      props.eurosportCyclingStaticDdb.grantReadWriteData(ddbStaticDs);
      props.eurosportCyclingLiveDdb.grantReadWriteData(ddbLiveDs);

      const ddbStaticResolverFunction = new lambda.Function(
        this,
        'ddbStaticResolverFunction',
        {
          runtime: lambda.Runtime.NODEJS_12_X,
          functionName: envSpecific('eurosport-cycling-ddb-static-resolver-function'),
          handler: 'index.handler',
          code: lambda.Code.fromAsset('./lib/lambda/node/ddb-resolver/dist'),
          memorySize: 256,
          timeout: Duration.seconds(30),
          tracing: Tracing.ACTIVE,
          environment: {
            TABLE_NAME_STATIC: props.eurosportCyclingStaticDdb.tableName,
            TABLE_NAME_LIVE: props.eurosportCyclingLiveDdb.tableName,
          },
        },
      );

      props.eurosportCyclingStaticDdb.grantReadWriteData(ddbStaticResolverFunction);
      props.eurosportCyclingLiveDdb.grantReadWriteData(ddbStaticResolverFunction);

      const invokeMutationFunction = new lambda.Function(
        this,
        'invokeMutationFunction',
        {
          runtime: lambda.Runtime.NODEJS_12_X,
          functionName: envSpecific('eurosport-cycling-invoke-mutation-function'),
          handler: 'index.handler',
          code: lambda.Code.fromAsset('./lib/lambda/node/invoke-mutation/dist'),
          memorySize: 128,
          timeout: Duration.seconds(5),
          tracing: Tracing.ACTIVE,
          environment: {
            API_KEY: this.eurosportCyclingApi.apiKey!,
            API_URL: this.eurosportCyclingApi.graphqlUrl,
          },
        },
      );

      // STATE MACHINE

      const configureCount = new sfn.Pass(this, 'ConfigureCount', {
        result: sfn.Result.fromObject({
          index: 0,
          count: 6,
        }),
        resultPath: '$.iterator',
      });

      const iterator = new tasks.LambdaInvoke(this, 'Iterator', {
        lambdaFunction: invokeMutationFunction,
        outputPath: '$.Payload',
        retryOnServiceExceptions: false,
      });

      const wait = new sfn.Wait(this, 'Wait', {
        time: sfn.WaitTime.duration(Duration.seconds(10)),
      }).next(iterator);

      const finalStatus = new sfn.Succeed(this, 'Job Complete', {
        comment: 'Job is complete',
      });

      const isCountReached = new sfn.Choice(this, 'IsCountReached')
        .when(sfn.Condition.booleanEquals('$.iterator.continue', true), wait)
        .otherwise(finalStatus);

      new sfn.StateMachine(this, 'StateMachine', {
        definition: configureCount.next(iterator).next(isCountReached),
        timeout: Duration.minutes(5),
      });

      /// END OF STATE MACHINE

      const ddbResolverDs = this.eurosportCyclingApi.addLambdaDataSource(
        'ddbResolverDs',
        ddbStaticResolverFunction,
      );

      const wordpressHttpApiDs = this.eurosportCyclingApi.addHttpDataSource(
        'wordpressHttpApiDs',
        resolverConfig.wordpressEndpoint,
      );

      this.datasources = {
        ddbResolverDs,
        ddbStaticDs,
        ddbLiveDs,
        wordpressHttpApiDs,
      };

      this.addResolvers();

      new CfnOutput(this, 'GraphUrl', {
        value: this.eurosportCyclingApi.graphqlUrl,
      });
    }

    private addResolvers() {
      Object.keys(resolverConfig.dataSources as [string]).forEach((ds) => {
        const dataSource: DataSource = resolverConfig.dataSources[ds];

        Object.keys(dataSource).forEach((type) => {
          resolverConfig.dataSources[ds][type].forEach((query: any) => {
            if (query.type === 'vtl') {
              this.addVtlResolver(type, query, ds);
            }
            if (query.type === 'lambda') {
              this.addLambdaResolver(type, query, ds);
            }
            if (query.type === 'pipeline') {
              this.addPipelineResolver(type, query);
            }
            if (query.type === 'http') {
              this.addHttpResolver(type, query, ds);
            }
          });
        });
      });
    }

    private addPipelineResolver(type: string, query: DataSourceResolver) {
      const idList: Array<AppsyncFunction> = [];
      query.functions.forEach((funcName) => {
        idList.push(this.appsyncFunctions[funcName]);
      });

      this.eurosportCyclingApi.createResolver({
        typeName: type,
        fieldName: query.name,
        requestMappingTemplate: query.request
          ? this.replaceDdbTableName(query.request)
          : undefined,
        responseMappingTemplate: query.response
          ? this.replaceDdbTableName(query.response)
          : undefined,
        pipelineConfig: idList,
      });
    }

    private addLambdaResolver(
      type: string,
      query: DataSourceResolver,
      ds: string,
    ) {
      this.eurosportCyclingApi.createResolver({
        dataSource: this.datasources[ds],
        typeName: type,
        fieldName: query.name,
        requestMappingTemplate: query.request
          ? MappingTemplate.fromFile(query.request)
          : undefined,
        responseMappingTemplate: query.response
          ? MappingTemplate.fromFile(query.response)
          : undefined,
      });
    }

    private addVtlResolver(type: string, query: DataSourceResolver, ds: string) {
      this.eurosportCyclingApi.createResolver({
        dataSource: this.datasources[ds],
        typeName: type,
        fieldName: query.name,
        requestMappingTemplate: this.replaceDdbTableName(query.request),
        responseMappingTemplate: this.replaceDdbTableName(query.response),
      });
    }

    private addHttpResolver(type: string, query: DataSourceResolver, ds: string) {
      const resolver = new CfnResolver(this, `resolver_${query.name}_${hashCode(query.request)}`, {
        apiId: this.eurosportCyclingApi.apiId,
        typeName: type,
        fieldName: query.name,
        cachingConfig: {
          ttl: 1200,
        },
        requestMappingTemplate: this.replaceDdbTableName(query.request).renderTemplate(),
        responseMappingTemplate: this.replaceDdbTableName(query.response).renderTemplate(),
        dataSourceName: this.datasources[ds].name,
      });

      resolver.node.addDependency(this.eurosportCyclingApi);
      resolver.node.addDependency(this.cache);
    }

    private replaceDdbTableName(templatePath: string) {
      let data = fs.readFileSync(templatePath, 'utf8');
      data = data.replace('<<DDB_STATIC_TABLE_NAME>>', this.ctx.eurosportCyclingStaticDdb.tableName).replace('<<DDB_LIVE_TABLE_NAME>>', this.ctx.eurosportCyclingLiveDdb.tableName);
      return MappingTemplate.fromString(data);
    }
}
