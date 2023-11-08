import * as v from 'shared/src/valita.js';
import {encryptedBytesSchema} from './bytes.js';
import {firestoreDataConverter} from './converter.js';
import {appPath, deploymentOptionsSchema} from './deployment.js';
import * as path from './path.js';

// Name of the SecretManager secret that stores the base64url-encoded
// key for encrypting app secrets at rest.
export const ENCRYPTION_KEY_SECRET_NAME = 'APP_SECRETS_ENCRYPTION_KEY';

export const secretsSchema = v.record(encryptedBytesSchema);

export type Secrets = v.Infer<typeof secretsSchema>;

export const envSchema = v.object({
  deploymentOptions: deploymentOptionsSchema,

  // Encrypted payloads of secrets stored on behalf of the app. Some of these are
  // internal, such as the `REFLECT_AUTH_API_TOKEN`, and some are "Environment Variables"
  // specified by the app developer, transmitted to the Worker via Cloudflare
  // secrets (using the `REFLECT_VAR_` prefix to distinguish them from internal
  // vars / bindings).
  secrets: secretsSchema,
});

export type Env = v.Infer<typeof envSchema>;

export const envDataConverter = firestoreDataConverter(envSchema);

export const ENVS_COLLECTION_ID = 'envs';

// The name of the default Env. This purposely contains characters
// that will be considered invalid for user-defined envs (namely,
// parentheses will be disallowed since they can't be in hostnames)
// in order to avoid collisions.
export const DEFAULT_ENV = '(default)';

export function envsCollection(appID: string) {
  return path.append(appPath(appID), ENVS_COLLECTION_ID);
}

export function envPath(appID: string, name: string) {
  return path.append(envsCollection(appID), name);
}
