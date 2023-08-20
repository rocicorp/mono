import {defineSecret} from 'firebase-functions/params';
import type {DeploymentSecrets} from 'mirror-schema/src/deployment.js';
import {sha256OfString} from 'mirror-schema/src/module.js';
import {nanoid} from 'nanoid';

const datadogLogsApiKey = defineSecret('DATADOG_LOGS_API_KEY');
const datadogMetricsApiKey = defineSecret('DATADOG_METRICS_API_KEY');

export async function getAppSecrets() {
  const secrets: DeploymentSecrets = {
    /* eslint-disable @typescript-eslint/naming-convention */
    REFLECT_AUTH_API_KEY: nanoid(), // TODO(darick): Replace with a stable per-app secret.
    DATADOG_LOGS_API_KEY: datadogLogsApiKey.value(),
    DATADOG_METRICS_API_KEY: datadogMetricsApiKey.value(),
    /* eslint-enable @typescript-eslint/naming-convention */
  };
  const hashes = await hashSecrets(secrets);
  return {secrets, hashes};
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
