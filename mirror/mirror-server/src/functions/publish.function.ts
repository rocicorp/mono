import type {Firestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {defineSecret, defineString} from 'firebase-functions/params';
import {
  publishRequestSchema,
  publishResponseSchema,
} from 'mirror-protocol/src/publish.js';
import type {CfModule} from '../cloudflare/create-worker-upload-form.js';
import {publish as publishToCloudflare} from '../cloudflare/publish.js';
import {withAuthorization} from './validators/auth.js';
import {withSchema} from './validators/schema.js';

// This is the API token for reflect-server.net
// https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/
const cloudflareApiToken = defineSecret('CLOUDFLARE_API_TOKEN');
const cloudflareAccountId = defineString('CLOUDFLARE_ACCOUNT_ID');

export const publish = (
  firestore: Firestore,
  storage: Storage,
  bucketName: string,
) =>
  withSchema(
    publishRequestSchema,
    publishResponseSchema,
    withAuthorization(async publishRequest => {
      const appName = publishRequest.name;

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
        bucketName,
        config,
        sourceModule,
        sourcemapModule,
        appName,
        publishRequest.desiredVersion,
      );

      return {success: true};
    }),
  );
