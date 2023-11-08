import {compareUTF8} from 'compare-utf8';
import type {Firestore} from 'firebase-admin/firestore';
import {
  listVarsRequestSchema,
  listVarsResponseSchema,
} from 'mirror-protocol/src/vars.js';
import type {EncryptedBytes} from 'mirror-schema/src/bytes.js';
import {DEFAULT_ENV, envDataConverter, envPath} from 'mirror-schema/src/env.js';
import {SERVER_VARIABLE_PREFIX} from 'mirror-schema/src/vars.js';
import {SecretsCache, SecretsClient} from '../../secrets/index.js';
import {decryptSecrets} from '../app/secrets.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {getDataOrFail} from '../validators/data.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';

export const list = (firestore: Firestore, secretsClient: SecretsClient) =>
  validateSchema(listVarsRequestSchema, listVarsResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async request => {
      const secrets = new SecretsCache(secretsClient);
      const {appID, decrypted} = request;

      const env = await firestore
        .doc(envPath(appID, DEFAULT_ENV))
        .withConverter(envDataConverter)
        .get();
      const {secrets: appSecrets} = getDataOrFail(
        env,
        'internal',
        `Missing environment for App ${appID}`,
      );

      const vars = Object.entries(appSecrets)
        .filter(([name]) => name.startsWith(SERVER_VARIABLE_PREFIX))
        .map(
          ([name, val]) =>
            [name.substring(SERVER_VARIABLE_PREFIX.length), val] as [
              string,
              EncryptedBytes,
            ],
        )
        .sort(([a], [b]) => compareUTF8(a, b));

      if (!decrypted) {
        return {
          success: true,
          decrypted: false,
          envs: {
            ['(default)']: {
              vars: Object.fromEntries(vars.map(([name]) => [name, '*****'])),
            },
          },
        };
      }
      const decryptedVars = await decryptSecrets(
        secrets,
        Object.fromEntries(vars),
      );
      return {
        success: true,
        decrypted: true,
        envs: {
          ['(default)']: {
            vars: decryptedVars,
          },
        },
      };
    });
