#!/usr/bin/env node

import {
  DOInstances,
  DONamespaces,
  listDOInstances,
  listDONamespaces,
} from '../src/cloudflare/api.js';
import {AUTH_ROUTES} from '../src/server/auth-do.js';
import type {RoomRecord} from '../src/server/rooms.js';

// FYI argv[0] is node.
if (process.argv.length < 3) {
  console.error(`Usage: ${process.argv[1]} <COMMAND>`);
  console.error(
    'Commands:\n' +
      '  list-do-namespaces [--class=<CLASS> [--script=<SCRIPT>]]\n' +
      '  list-do-instances --namespace-id=<NAMESPACE ID>\n' +
      '  validate-rooms --script=<SCRIPT>',
  );
  process.exit(1);
}

const accountID = process.env['CF_ACCOUNT_ID'];
if (accountID === undefined || accountID === '') {
  console.error(
    'CF_ACCOUNT_ID must be set.\n' +
      'The account ID is the hex string in the URL when you select your account at https://dash.cloudflare.com/',
  );
  process.exit(1);
}

const cfApiToken = process.env['CF_API_TOKEN'];
if (cfApiToken === undefined || cfApiToken === '') {
  console.error(
    'CF_API_TOKEN must be set. To create one:\n' +
      '1. Go to https://dash.cloudflare.com/profile/api-tokens\n' +
      "2. Click 'Create Token'\n" +
      "3. Select template 'Edit Cloudflare Workers'\n" +
      "4. Select the account under 'Account Resources'\n" +
      "5. Under 'Zone Resources' select 'All zones'\n" +
      "6. Optionally configure 'Client IP Address Filtering'\n" +
      "7. Click 'Continue to summary'\n" +
      "8. Click 'Create Token'\n",
  );
  process.exit(1);
}

// CLI parsing is brittle. It expects the flags to be in the right order.
switch (process.argv[2]) {
  case 'list-do-namespaces':
    await runListDONamespaces(process.argv.slice(3));
    break;
  case 'list-do-instances':
    await runListDOInstances(process.argv.slice(3));
    break;
  case 'validate-rooms':
    await runValidateRooms(process.argv.slice(3));
    break;
  default:
    console.error('Unknown command: ' + process.argv[2]);
    process.exit(1);
}

async function runListDONamespaces(argv: string[]) {
  let cls: string | undefined = undefined;
  let script: string | undefined = undefined;
  if (argv.length >= 1) {
    cls = parseNextFlag(argv[0], 'class');
  }
  if (argv.length === 2) {
    script = parseNextFlag(argv[1], 'script');
  }
  const result = await getDONamespaces(cls, script);
  console.log(result);
}

async function getDONamespaces(
  cls: string | undefined,
  script: string | undefined,
): Promise<DONamespaces> {
  let result = (await listDONamespaces(
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    accountID!,
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    cfApiToken!,
  )) as DONamespaces;
  if (result === null) {
    console.error('No DO namespace result');
    process.exit(1);
  }
  if (cls !== undefined) {
    result = result.filter((ns: {class: string}) => ns.class === cls);
  }
  if (script !== undefined) {
    result = result.filter((ns: {script: string}) => ns.script === script);
  }
  return Promise.resolve(result);
}

async function runListDOInstances(argv: string[]) {
  if (argv.length !== 1) {
    console.error(`Must pass --namespace-id=<ID>`);
    process.exit(1);
  }
  const nsID = parseNextFlag(argv[0], 'namespace-id');
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const result = await getDOInstances(nsID!);
  console.log(result);
  // TODO check if hasStoredData is false?
}

async function getDOInstances(nsID: string) {
  const result = await listDOInstances(
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    accountID!,
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    cfApiToken!,
    nsID,
  );
  if (result === null) {
    console.error('No DO instances result');
    process.exit(1);
  }
  return Promise.resolve(result);
}

async function runValidateRooms(argv: string[]) {
  const cls = 'RoomDO';
  if (argv.length !== 1) {
    console.error(`Must pass --script=<SCRIPT NAME>`);
    process.exit(1);
  }
  const script = parseNextFlag(argv[0], 'script');

  let reflectURL = process.env['REFLECT_URL'];
  if (reflectURL === undefined || reflectURL === '') {
    console.error(
      'REFLECT_URL must be set.\n' + 'e.g. https://acme.workers.dev',
    );
    process.exit(1);
  }
  if (reflectURL[reflectURL.length - 1] === '/') {
    reflectURL = reflectURL.slice(0, -1);
  }

  const reflectAuthApiToken = process.env['REFLECT_AUTH_API_TOKEN'];
  if (reflectAuthApiToken === undefined || reflectAuthApiToken === '') {
    console.error('REFLECT_AUTH_API_TOKEN must be set.');
    process.exit(1);
  }

  const nsResult = await getDONamespaces(cls, script);
  if (nsResult === null) {
    console.error('No DO namespaces result');
    process.exit(1);
  }
  if (nsResult.length !== 1) {
    console.error('Expected exactly one DO namespace');
    process.exit(1);
  }
  const nsID = nsResult[0].id;

  const instancesResult = (await getDOInstances(nsID)) as DOInstances;
  console.log(
    `CF has ${instancesResult.length} DO room instances. Validating...`,
  );

  const roomRecords = (await jsonFetchWithAuthApiKey(
    `${reflectURL}${AUTH_ROUTES.roomRecords}`,
    reflectAuthApiToken,
  )) as Array<RoomRecord>;
  const roomRecordsByOjbectID = new Map(
    roomRecords.map(rr => [rr.objectIDString, rr]),
  );
  const missingRoomRecords = new Set<string>();
  for (const instance of instancesResult) {
    if (!roomRecordsByOjbectID.has(instance.id)) {
      missingRoomRecords.add(instance.id);
    }
  }
  console.log(
    `${missingRoomRecords.size} CF RoomDO instances are missing room records:`,
  );
  console.log(missingRoomRecords);
  const extraRoomRecords = new Set<string>();
  for (const roomRecord of roomRecords) {
    if (!instancesResult.some(i => i.id === roomRecord.objectIDString)) {
      extraRoomRecords.add(roomRecord.objectIDString);
    }
  }
  console.log(
    `${extraRoomRecords.size} reflect room records have no CF RoomDO instance:`,
  );
  console.log(extraRoomRecords);
  if (missingRoomRecords.size === 0 || extraRoomRecords.size === 0) {
    console.log('Validation successful');
  }
}

// Note: brittle!
function parseNextFlag(arg: string, flag: string): string | undefined {
  const match = arg.match(new RegExp(`^--${flag}=([a-zA-Z0-9_-]+)$`));
  if (match === null) {
    console.error(`Error parsing: ${arg}`);
    process.exit(1);
  }
  return match[1];
}

async function jsonFetchWithAuthApiKey(url: string, apiToken: string) {
  const resp = await fetch(url, {
    headers: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'x-reflect-api-key': `${apiToken}`,
    },
  });
  if (!resp.ok) {
    console.error(`Error fetching ${url}: ${resp.status} ${resp.statusText}`);
    process.exit(1);
  }
  return resp.json();
}
