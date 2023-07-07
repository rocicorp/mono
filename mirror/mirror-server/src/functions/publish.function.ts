import type {Firestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {defineSecret, defineString} from 'firebase-functions/params';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  publishRequestSchema,
  publishResponseSchema,
} from 'mirror-protocol/src/publish.js';
import * as semver from 'semver';
import {isSupportedSemverRange} from 'shared/src/is-supported-semver-range.js';
import type {CfModule} from '../cloudflare/create-worker-upload-form.js';
import {publish as publishToCloudflare} from '../cloudflare/publish.js';
import {findNewestMatchingVersion} from '../find-newest-matching-version.js';
import {withAuthorization} from './validators/auth.js';
import {withSchema} from './validators/schema.js';

// This is the API token for reflect-server.net
// https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/
const cloudflareApiToken = defineSecret('CLOUDFLARE_API_TOKEN');
const cloudflareAccountId = defineString('CLOUDFLARE_ACCOUNT_ID');

export const publish = (
  firestore: Firestore,
  storage: Storage,
  _bucketName: string,
) =>
  withSchema(
    publishRequestSchema,
    publishResponseSchema,
    withAuthorization(async publishRequest => {
      const {serverVersionRange, name: appName} = publishRequest;

      if (semver.validRange(serverVersionRange) === null) {
        throw new HttpsError('invalid-argument', 'Invalid desired version');
      }

      const range = new semver.Range(serverVersionRange);
      if (!isSupportedSemverRange(range)) {
        throw new HttpsError('invalid-argument', 'Unsupported desired version');
      }

      const version = await findNewestMatchingVersion(firestore, range);
      console.log(
        `Found matching version for ${serverVersionRange}: ${version}`,
      );

      const config = {
        accountID: cloudflareAccountId.value(),
        // TODO(arv): This is not the right name.
        scriptName: appName,
        apiToken: cloudflareApiToken.value(),
      } as const;
      const sourceModule: CfModule = {
        ...publishRequest.source,
        type: 'esm',
      };
      const sourcemapModule: CfModule = {
        ...publishRequest.sourcemap,
        type: 'text',
      };

      await publishToCloudflare(
        firestore,
        storage,
        config,
        sourceModule,
        sourcemapModule,
        appName,
        version,
      );

      return {success: true};
    }),
  );
