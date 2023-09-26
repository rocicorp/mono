import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {getSecret} from './secrets.js';
import {getFirestore} from 'firebase-admin/firestore';
import {
  providerPath,
  providerDataConverter,
  type Provider,
} from 'mirror-schema/src/provider.js';

export type ProviderConfig = Provider & {
  apiKey: string;
};

export async function getProviderConfig(
  yargs: YargvToInterface<CommonYargsArgv>,
): Promise<ProviderConfig> {
  const {stack, provider} = yargs;
  const cloudflare = (
    await getFirestore()
      .doc(providerPath(provider))
      .withConverter(providerDataConverter)
      .get()
  ).data();
  if (!cloudflare) {
    throw new Error(`No "${provider}" provider is setup for ${stack}`);
  }

  const apiKey = await getSecret(stack, `${provider}_api_token`);
  return {...cloudflare, apiKey};
}
