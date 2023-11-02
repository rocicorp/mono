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
import {
  ALLOWED_SERVER_VARIABLE_CHARS,
  MAX_SERVER_VARIABLES,
  SERVER_VARIABLE_PREFIX,
  variableIsWithinSizeLimit,
} from 'mirror-schema/src/vars.js';
import {SecretsCache, SecretsClient} from '../../secrets/index.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {getDataOrFail} from '../validators/data.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';
import {deploymentAtOrAfter} from './shared.js';

export const set = (firestore: Firestore, secretsClient: SecretsClient) =>
  validateSchema(setVarsRequestSchema, setVarsResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (request, context) => {
      const secrets = new SecretsCache(secretsClient);
      const {appID, vars} = request;
      const {
        app: {name},
      } = context;

      for (const [name, value] of Object.entries(vars)) {
        if (!ALLOWED_SERVER_VARIABLE_CHARS.test(name)) {
          throw new HttpsError(
            'invalid-argument',
            'Variable names can only contain alphanumeric characters and underscores',
          );
        }
        if (!variableIsWithinSizeLimit(name, value)) {
          throw new HttpsError(
            'invalid-argument',
            'UTF-8 encoded Variables must be within 5 kilobytes',
          );
        }
      }

      const encryptionKey = await secrets.getSecret(ENCRYPTION_KEY_SECRET_NAME);
      const encrypted = Object.fromEntries(
        Object.entries(vars).map(
          ([name, value]) =>
            [
              `${SERVER_VARIABLE_PREFIX}${name}`,
              encryptUtf8(
                value,
                Buffer.from(encryptionKey.payload, 'base64url'),
                {version: encryptionKey.version},
              ),
            ] as [string, EncryptedBytes],
        ),
      ) as AppSecrets;

      const appDoc = firestore
        .doc(appPath(appID))
        .withConverter(appDataConverter);

      const runningDeployment = await firestore.runTransaction(async tx => {
        const {secrets: currSecrets, runningDeployment} = getDataOrFail(
          await tx.get(appDoc),
          'not-found',
          `App ${name} was deleted`,
        );
        const mergedSecrets = {
          ...currSecrets,
          ...encrypted,
        };
        if (
          Object.keys(mergedSecrets).filter(name =>
            name.startsWith(SERVER_VARIABLE_PREFIX),
          ).length > MAX_SERVER_VARIABLES
        ) {
          throw new HttpsError(
            'resource-exhausted',
            `Up to ${MAX_SERVER_VARIABLES} Server Variables are allowed.\n` +
              `Use 'npx @rocicorp/reflect vars delete' to remove unused variables.`,
          );
        }
        tx.set(appDoc, {secrets: mergedSecrets}, {merge: true});
        return runningDeployment;
      });
      if (!runningDeployment) {
        // No deployment to re-deploy.
        return {success: true};
      }
      // Read the app back to get the write time. Note that the deployment
      // may have been written to the doc already.
      const app = await appDoc.get();
      if (!app.updateTime) {
        throw new HttpsError('not-found', `App ${name} was deleted`);
      }
      return deploymentAtOrAfter(firestore, appID, app.updateTime);
    });
