#!/usr/bin/env node
import * as cdk from '@aws-cdk/core';
import { EurosportCyclingStorage } from '../lib/storage-stack';
import { EurosportCyclingProcessing } from '../lib/processing-stack';
import { EurosportCyclingConsumption } from '../lib/consumption-stack';
import { EurosportCyclingDataGen } from '../lib/datagen-stack';
import { EurosportCyclingOD } from '../lib/od-stack';
import { envSpecific } from '../lib/constructs/helpers';
import { EurosportCyclingFrontend } from '../lib/frontend-stack';

const streamName = envSpecific('EurosportCyclingLiveStream');

const app = new cdk.App();
// Create storage stack and get variables for S3 buckets
const StorageStack = new EurosportCyclingStorage(app, envSpecific('EurosportCyclingStorage'), {
  description: 'This stack creates static items.',
});

// Create dependend stack with S3 bucktes from the stack above
new EurosportCyclingProcessing(app, envSpecific('EurosportCyclingProcessing'), {
  ddbTableStatic: StorageStack.ddbTableStatic,
  ddbTableLive: StorageStack.ddbTableLive,
  s3raw: StorageStack.bucketRaw,
  s3staging: StorageStack.bucketStaging,
  s3poststaging: StorageStack.bucketPostStaging,
  s3Temp: StorageStack.bucketTemp,
  description: 'This stack creates replacable items.',
  streamName,
});

// Create dependend stack with S3 bucktes from the stack above
const consumptionStack = new EurosportCyclingConsumption(app, envSpecific('EurosportCyclingConsumption'), {
  ddbTableStatic: StorageStack.ddbTableStatic,
  ddbTableLive: StorageStack.ddbTableLive,
  s3raw: StorageStack.bucketRaw,
  s3Swagger: StorageStack.bucketSwagger,
  description: 'This stack creates consumption layer',
  streamName,
});

new EurosportCyclingDataGen(app, envSpecific('EurosportCyclingDataGen'), {
  apiURL: consumptionStack.apiURL,
  inApiKeySecret: consumptionStack.inApiKeySecret,
});

const odStack = new EurosportCyclingOD(app, envSpecific('EurosportCyclingOD'), {});

new EurosportCyclingFrontend(app, envSpecific('EurosportCyclingFront'), {
  bucket: odStack.bucketOD,
  distribution: odStack.distribution,
});
