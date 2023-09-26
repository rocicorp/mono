import {confirm, password, select} from './inquirer.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {Accounts} from 'cloudflare-api/src/accounts.js';
import {Zones, Zone} from 'cloudflare-api/src/zones.js';
import {
  providerPath,
  providerDataConverter,
  type Provider,
} from 'mirror-schema/src/provider.js';
import {assert} from 'shared/src/asserts.js';
import {storeSecret} from './secrets.js';
import {getFirestore} from 'firebase-admin/firestore';

export function configureProviderOptions(yargs: CommonYargsArgv) {
  return yargs
    .positional('id', {
      desc: 'Provider id. Defaults to "default"',
      type: 'string',
      default: 'default',
    })
    .option('namespace', {
      desc: 'Dispatch namespace to host workers in',
      type: 'string',
      default: 'prod',
    })
    .option('max-apps', {
      desc: 'The default number of Apps each Team is allowed to create',
      type: 'number',
      default: 3,
    });
}

type ConfigureProviderHandlerArgs = YargvToInterface<
  ReturnType<typeof configureProviderOptions>
>;

export async function configureProviderHandler(
  yargs: ConfigureProviderHandlerArgs,
): Promise<void> {
  const {id, stack, namespace, maxApps} = yargs;
  const apiToken = await password({
    message: 'Enter the Cloudflare API token for the provider:',
  });
  const accounts = await new Accounts(apiToken).list();
  const account = await selectOneOf('Cloudflare Account', accounts);
  const zones = await new Zones(apiToken).list();
  const zone = await selectOneOf('Zone', zones);

  checkPermissions(zone);

  // TODO: Verify that the account can perform the necessary functions
  // by creating (and cleaning up) temporary records, hostnames, etc.
  // - Creating a Custom Hostname with metadata
  // - Creating a DNS Record with tags

  const provider: Provider = {
    accountID: account.id,
    dispatchNamespace: namespace,
    defaultMaxApps: maxApps,
    defaultZone: {
      id: zone.id,
      name: zone.name,
    },
  };
  console.log(`Configuring "${id}" provider`, provider);
  if (!(await confirm({message: `Continue`, default: true}))) {
    console.warn(`Action aborted`);
    process.exit(-1);
  }
  console.log(`Storing API token`);
  await storeSecret(stack, `${id}_api_token`, apiToken);

  const firestore = getFirestore();
  await firestore
    .doc(providerPath(id))
    .withConverter(providerDataConverter)
    .set(provider);
  console.log(`Successfully configured "${id}" provider`);
}

const REQUIRED_PERMISSIONS = [
  '#zone_settings:read',
  '#zone_settings:edit',
  '#dns_records:read',
  '#dns_records:edit',
  '#ssl:read',
  '#ssl:edit',
  '#zone:edit',
  '#zone:read',
  '#worker:edit',
  '#worker:read',
] as const;

function checkPermissions(zone: Zone) {
  const granted = new Set(zone.permissions);
  const missing = REQUIRED_PERMISSIONS.filter(perm => !granted.has(perm));
  if (missing.length) {
    throw new Error(
      `API token is missing permissions [${missing}] for zone [${zone.permissions}]`,
    );
  }
}

async function selectOneOf<T extends {name: string}>(
  type: string,
  choices: T[],
): Promise<T> {
  assert(choices.length, `No ${type}s accessible from the API token`);
  const choice =
    choices.length === 1
      ? choices[0]
      : await select({
          message: `Select the ${type}`,
          choices: choices.map(choice => ({
            name: choice.name,
            value: choice,
          })),
        });
  console.log(`Selected ${type}:`, choice);
  return choice;
}
