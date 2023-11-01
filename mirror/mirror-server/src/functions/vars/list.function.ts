import {compareUTF8} from 'compare-utf8';
import type {Firestore} from 'firebase-admin/firestore';
import {
  listVarsRequestSchema,
  listVarsResponseSchema,
} from 'mirror-protocol/src/vars.js';
import type {EncryptedBytes} from 'mirror-schema/src/bytes.js';
import {SecretsCache, SecretsClient} from '../../secrets/index.js';
import {getAppSecrets} from '../app/secrets.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';
import {SERVER_VAR_PREFIX} from './shared.js';

export const list = (firestore: Firestore, secretsClient: SecretsClient) =>
  validateSchema(listVarsRequestSchema, listVarsResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (request, context) => {
      const secrets = new SecretsCache(secretsClient);
      const {decrypted} = request;
      const {
        app: {secrets: appSecrets},
      } = context;

      const vars = Object.entries(appSecrets)
        .filter(([name]) => name.startsWith(SERVER_VAR_PREFIX))
        .map(
          ([name, val]) =>
            [name.substring(SERVER_VAR_PREFIX.length), val] as [
              string,
              EncryptedBytes,
            ],
        )
        .sort(([a], [b]) => compareUTF8(a, b));

      if (!decrypted) {
        return {
          success: true,
          decrypted: false,
          vars: Object.fromEntries(vars.map(([name]) => [name, '*****'])),
        };
      }
      const {secrets: decryptedVars} = await getAppSecrets(
        secrets,
        Object.fromEntries(vars),
        false,
      );
      return {
        success: true,
        decrypted: true,
        vars: decryptedVars,
      };
    });
