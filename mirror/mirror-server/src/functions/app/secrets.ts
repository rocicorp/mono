import {defineSecret} from 'firebase-functions/params';
import {
  ENCRYPTION_KEY_SECRET_NAME,
  type AppSecrets,
} from 'mirror-schema/src/app.js';
import {decryptUtf8} from 'mirror-schema/src/crypto.js';
import type {DeploymentSecrets} from 'mirror-schema/src/deployment.js';
import {assert} from 'shared/src/asserts.js';
import {sha256OfString} from 'shared/src/sha256.js';
import type {Secrets} from '../../secrets/index.js';

export function defineSecretSafely(name: string) {
  const secret = defineSecret(name);
  return Object.freeze({
    name,
    value: () => {
      // This mirrors the code in Firestore's SecretParam.runtimeValue() that checks that the cloud function has
      // been properly configured to use the secret:
      // https://github.com/firebase/firebase-functions/blob/3e7a4b77967e46a067712445c499c5df005b8e31/src/params/types.ts#L380
      //
      // with the difference of handling a misconfiguration with assertion error rather than simply
      // logging a warning and returning the empty string. Failure to properly read the secret can
      // otherwise lead to problems ranging from hard-to-debug (logs don't appear in datadog, but we
      // can't see secrets in Cloudflare) to really-bad (infinite redeployment loops because deployed
      // secrets don't match the expected ones).
      assert(
        process.env[secret.name] !== undefined,
        `No value found for secret parameter "${secret.name}". A function can only access a secret if you include the secret in the function's dependency array.`,
      );
      return secret.value();
    },
  });
}

const datadogLogsApiKey = defineSecretSafely('DATADOG_LOGS_API_KEY');
const datadogMetricsApiKey = defineSecretSafely('DATADOG_METRICS_API_KEY');

// TODO(darick): Find the right place for this constant. Somewhere in packages/reflect* ?
export const REFLECT_AUTH_API_KEY = 'REFLECT_AUTH_API_KEY';

export const DEPLOYMENT_SECRETS_NAMES = [
  'DATADOG_LOGS_API_KEY',
  'DATADOG_METRICS_API_KEY',
] as const;

export async function getAppSecrets(
  secrets: Secrets,
  encrypted: AppSecrets,
): Promise<{
  secrets: Record<string, string>;
  hashes: Record<string, string>;
}> {
  // Generate the hashes from the datadog keys and from the app secret ciphertexts.
  // It is important that we don't hash the plaintexts as that could leak information
  // about equality between secrets.
  const decoder = new TextDecoder('utf-8');
  const hashes = hashSecrets({
    ['DATADOG_LOGS_API_KEY']: datadogLogsApiKey.value(),
    ['DATADOG_METRICS_API_KEY']: datadogMetricsApiKey.value(),
    ...Object.fromEntries(
      Object.entries(encrypted).map(([name, val]) => [
        name,
        decoder.decode(val.ciphertext),
      ]),
    ),
  });

  const decrypted = await Promise.all(
    Object.entries(encrypted).map(async ([name, val]) => {
      const {secretName, version} = val.key;
      const {payload: encryptionKey} = await secrets.getSecret(
        secretName ?? ENCRYPTION_KEY_SECRET_NAME,
        version,
      );
      return [name, decryptUtf8(val, Buffer.from(encryptionKey, 'base64url'))];
    }),
  );

  return {
    secrets: {
      ['DATADOG_LOGS_API_KEY']: datadogLogsApiKey.value(),
      ['DATADOG_METRICS_API_KEY']: datadogMetricsApiKey.value(),
      ...Object.fromEntries(decrypted),
    },
    hashes: await hashes,
  };
}

export async function hashSecrets(
  secrets: DeploymentSecrets,
): Promise<DeploymentSecrets> {
  const hashes = {...secrets};
  const hashSecret = async (key: keyof DeploymentSecrets) => {
    hashes[key] = await sha256OfString(secrets[key]);
  };
  await Promise.all(
    Object.keys(secrets).map(key => hashSecret(key as keyof DeploymentSecrets)),
  );
  return hashes;
}
