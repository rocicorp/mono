import type {Firestore} from 'firebase-admin/firestore';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  setVarsRequestSchema,
  setVarsResponseSchema,
} from 'mirror-protocol/src/vars.js';
import {
  AppSecrets,
  ENCRYPTION_KEY_SECRET_NAME,
  appDataConverter,
  appPath,
} from 'mirror-schema/src/app.js';
import type {EncryptedBytes} from 'mirror-schema/src/bytes.js';
import {encryptUtf8} from 'mirror-schema/src/crypto.js';
import {SecretsCache, SecretsClient} from '../../secrets/index.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';
import {SERVER_VAR_PREFIX, deploymentAfter} from './shared.js';

const ALLOWED_VAR_CHARS = /^[A-Za-z0-9_]+$/;

export const set = (firestore: Firestore, secretsClient: SecretsClient) =>
  validateSchema(setVarsRequestSchema, setVarsResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (request, context) => {
      const secrets = new SecretsCache(secretsClient);
      const {appID, vars} = request;
      const {
        app: {runningDeployment},
      } = context;

      for (const name of Object.keys(vars)) {
        if (!ALLOWED_VAR_CHARS.test(name)) {
          throw new HttpsError(
            'invalid-argument',
            'Server Variable names can only contain alphanumeric characters and underscores',
          );
        }
      }

      const encryptionKey = await secrets.getSecret(ENCRYPTION_KEY_SECRET_NAME);
      const encrypted = Object.fromEntries(
        Object.entries(vars).map(
          ([name, value]) =>
            [
              `${SERVER_VAR_PREFIX}${name}`,
              encryptUtf8(
                value,
                Buffer.from(encryptionKey.payload, 'base64url'),
                {version: encryptionKey.version},
              ),
            ] as [string, EncryptedBytes],
        ),
      ) as AppSecrets;
      const result = await firestore
        .doc(appPath(appID))
        .withConverter(appDataConverter)
        .set(
          {secrets: encrypted},
          {mergeFields: Object.keys(encrypted).map(name => `secrets.${name}`)},
        );
      if (!runningDeployment) {
        // No deployment to re-deploy.
        return {success: true};
      }
      return deploymentAfter(firestore, appID, result.writeTime);
    });
