import * as cdk from '@aws-cdk/core';
import { AwsCustomResource, PhysicalResourceId } from '@aws-cdk/custom-resources';
import { generateSesPolicyForCustomResource } from './ses-helper';

export interface IVerifySesEmailAddressProps {
  /**
   * The email address to be verified, e.g. 'hello@example.org'.
   */
  readonly emailAddress: string;
  readonly region?: string;
}

export class VerifySesEmailAddress extends cdk.Construct {
  constructor(scope: cdk.Construct, id: string, props: IVerifySesEmailAddressProps) {
    super(scope, id);

    const { emailAddress, region } = props;

    new AwsCustomResource(this, `VerifyEmailIdentity${emailAddress}`, {
      onCreate: {
        service: 'SES',
        action: 'verifyEmailIdentity',
        parameters: {
          EmailAddress: emailAddress,
        },
        physicalResourceId: PhysicalResourceId.of(`verify-${emailAddress}`),
        region,
      },
      onDelete: {
        service: 'SES',
        action: 'deleteIdentity',
        parameters: {
          Identity: emailAddress,
        },
        region,
      },
      policy: generateSesPolicyForCustomResource('VerifyEmailIdentity', 'DeleteIdentity'),
    });
  }
}
