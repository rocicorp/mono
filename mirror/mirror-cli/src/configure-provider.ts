import {confirm, password, select} from './inquirer.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {Accounts} from 'cloudflare-api/src/accounts.js';
import {CustomHostnames} from 'cloudflare-api/src/custom-hostnames.js';
import {DNSRecords} from 'cloudflare-api/src/dns-records.js';
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
    })
    .option('dry-run', {
      desc: 'Verify the API key, but do not store it or the resulting Provider.',
      type: 'boolean',
      default: false,
    });
}

type ConfigureProviderHandlerArgs = YargvToInterface<
  ReturnType<typeof configureProviderOptions>
>;

export async function configureProviderHandler(
  yargs: ConfigureProviderHandlerArgs,
): Promise<void> {
  const {id, stack, namespace, maxApps, dryRun} = yargs;
  const apiToken = await password({
    message: 'Enter the Cloudflare API token for the provider:',
  });
  const accounts = await new Accounts(apiToken).list();
  const account = await selectOneOf('Cloudflare Account', accounts);
  const zones = await new Zones(apiToken).list();
  const zone = await selectOneOf('Zone', zones);
  const {id: zoneID, name: zoneName} = zone;

  checkPermissions(zone);
  await checkCapabilities(
    zoneName,
    new CustomHostnames({apiToken, zoneID}),
    new DNSRecords({apiToken, zoneID}),
  );

  // TODO: Verify that the account can perform the necessary functions
  // by creating (and cleaning up) temporary records, hostnames, etc.
  // - Creating a Custom Hostname with metadata
  // - Creating a DNS Record with tags

  const provider: Provider = {
    accountID: account.id,
    dispatchNamespace: namespace,
    defaultMaxApps: maxApps,
    defaultZone: {zoneID, zoneName},
  };
  console.log(`Configuring "${id}" provider`, provider);
  if (dryRun || !(await confirm({message: `Continue`, default: true}))) {
    console.warn(`Action aborted ${dryRun ? '(--dry-run)' : ''}`);
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

async function checkCapabilities(
  zoneName: string,
  customHostnames: CustomHostnames,
  dnsRecords: DNSRecords,
): Promise<void> {
  console.debug(`Verifying Custom Hostname metadata capability`);
  const ch = await customHostnames.create({
    hostname: `test-mirror-hostame.${zoneName}`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    custom_metadata: {
      namespace: 'foo',
      script: 'bar',
    },
  });
  await customHostnames.delete(ch.id);

  console.debug(`Verifying DNS Record tagging capability`);
  // Create a TXT record with a tag to ensure that tags are enabled on the account.
  const record = await dnsRecords.create({
    type: 'TXT',
    name: 'test-mirror-record',
    content: 'test-mirror-content',
    comment: 'Temporarily created by mirror-cli. Delete me.',
    // TODO: Uncomment when Cloudflare enables tags for us.
    // tags: ['foo:bar'],
  });
  await dnsRecords.delete(record.id);
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
