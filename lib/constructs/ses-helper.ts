import { Effect, PolicyStatement } from '@aws-cdk/aws-iam';
import { AwsCustomResourcePolicy } from '@aws-cdk/custom-resources';

export function generateSesPolicyForCustomResource(...methods: string[]): AwsCustomResourcePolicy {

  return AwsCustomResourcePolicy.fromStatements([
    new PolicyStatement({
      actions: methods.map((method) => 'ses:' + method),
      effect: Effect.ALLOW,
      resources: ['*'],
    }),
  ]);
}