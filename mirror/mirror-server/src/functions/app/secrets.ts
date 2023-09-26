import {defineSecret} from 'firebase-functions/params';
import type {DeploymentSecrets} from 'mirror-schema/src/deployment.js';
import {assert} from 'shared/src/asserts.js';
import {sha256OfString} from 'shared/src/sha256.js';
import {SecretManagerServiceClient} from '@google-cloud/secret-manager';
import {projectId} from '../../config/index.js';

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

export const DEPLOYMENT_SECRETS_NAMES = [
  'DATADOG_LOGS_API_KEY',
  'DATADOG_METRICS_API_KEY',
] as const;

export async function getAppSecrets() {
  const secrets: DeploymentSecrets = {
    /* eslint-disable @typescript-eslint/naming-convention */
    REFLECT_AUTH_API_KEY: 'dummy-api-key', // TODO(darick): Replace with a stable per-app secret.
    DATADOG_LOGS_API_KEY: datadogLogsApiKey.value(),
    DATADOG_METRICS_API_KEY: datadogMetricsApiKey.value(),
    /* eslint-enable @typescript-eslint/naming-convention */
  };
  const hashes = await hashSecrets(secrets);
  return {secrets, hashes};
}

export const NULL_SECRETS: DeploymentSecrets = {
  /* eslint-disable @typescript-eslint/naming-convention */
  REFLECT_AUTH_API_KEY: '',
  DATADOG_LOGS_API_KEY: '',
  DATADOG_METRICS_API_KEY: '',
  /* eslint-enable @typescript-eslint/naming-convention */
} as const;

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

export function mockApiTokenForProvider(provider: string | undefined) {
  mockProvider = provider;
}
let mockProvider: string | undefined;

export async function getApiToken(provider: string): Promise<string> {
  if (provider === mockProvider) {
    return mockProvider;
  }
  const secrets = new SecretManagerServiceClient();
  const name = `projects/${projectId}/secrets/${provider}_api_token/versions/latest`;
  const [{payload}] = await secrets.accessSecretVersion({name});
  if (!payload || !payload.data) {
    throw new Error(`No data for ${provider} secret`);
  }
  const {data} = payload;
  return typeof data === 'string' ? data : Buffer.from(data).toString('utf-8');
}
