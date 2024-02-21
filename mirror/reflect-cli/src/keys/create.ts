import {
  Firestore,
  collection,
  getDocs,
  getFirestore,
  query,
  where,
} from 'firebase/firestore';
import {
  APP_CREATE_PERMISSION,
  APP_PUBLISH_PERMISSION,
} from 'mirror-schema/src/external/api-key.js';
import {
  APP_COLLECTION,
  appViewDataConverter,
} from 'mirror-schema/src/external/app.js';
import color from 'picocolors';

import {
  createApiKey,
  isValidApiKeyName,
  listApiKeys,
} from 'mirror-protocol/src/api-keys.js';
import {readAppConfig} from '../app-config.js';
import {Separator, checkbox, type Choice, type Item} from '../inquirer.js';
import {makeRequester} from '../requester.js';

import {must} from 'shared/src/must.js';
import type {AuthContext} from '../handler.js';
import {padColumns} from '../table.js';
import {getSingleTeam} from '../teams.js';
import type {CommonYargsArgv, YargvToInterface} from '../yarg-types.js';
import {CREATED_APPS} from './list.js';
import {getLogger} from '../logger.js';

export function createKeyOptions(yargs: CommonYargsArgv) {
  return yargs.positional('name', {
    describe:
      'Unique name for the key. Must be alphanumeric, optionally with hyphens.',
    type: 'string',
    demandOption: true,
  });
}

type CreateKeyHandlerArgs = YargvToInterface<
  ReturnType<typeof createKeyOptions>
>;

export async function createKeyHandler(
  yargs: CreateKeyHandlerArgs,
  authContext: AuthContext,
): Promise<void> {
  const {name} = yargs;
  if (!isValidApiKeyName(name)) {
    getLogger().error(
      color.yellow(`Invalid name "${name}"\n`) +
        'Names must be lowercased alphanumeric, starting with a letter and not ending with a hyphen.',
    );
    process.exit(-1);
  }

  const {userID} = authContext.user;
  const firestore = getFirestore();
  const teamID = await getSingleTeam(firestore, userID, 'admin');
  const requester = makeRequester(userID);

  const {allPermissions} = await listApiKeys.call({
    requester,
    teamID,
    show: false,
  });

  const {appIDs, perms} = await promptForKeyConfiguration(
    firestore,
    teamID,
    name,
    allPermissions,
  );

  const {value} = await createApiKey.call({
    requester,
    teamID,
    name,
    appIDs,
    permissions: Object.fromEntries(perms.map(perm => [perm, true])),
  });
  getLogger().log(`Created key "${color.bold(name)}": ${value}`);
}

export async function promptForKeyConfiguration(
  firestore: Firestore,
  teamID: string,
  keyName: string,
  allPermissions: Record<string, string>,
  permSelected: (perm: string) => boolean = () => false,
  appSelected: (app: AppInfo) => boolean = app => app.inAppDirectory,
): Promise<{appIDs: string[]; perms: string[]}> {
  const apps = await getApps(firestore, teamID);

  // The "app:create" permission is handled specially, shown as a
  // "(created apps)" option in the --- Authorized Apps --- section.
  // We try to encapsulate this in this function as much as possible
  // (though list.ts also contains some logic for the read side).
  const hasAppCreate = allPermissions[APP_CREATE_PERMISSION];
  delete allPermissions[APP_CREATE_PERMISSION];

  const desc = padColumns([
    ...Object.entries(allPermissions),
    [
      CREATED_APPS,
      `authorized for apps created by the key (requires ${APP_PUBLISH_PERMISSION})`,
    ],
  ]);
  const appCreateRow = must(desc.pop());
  if (hasAppCreate) {
    apps.unshift({
      name: `${appCreateRow[0]}     ${color.gray(appCreateRow[1])}`,
      id: CREATED_APPS,
      inAppDirectory: false,
    });
  }

  const allPerms = Object.keys(allPermissions);
  const selected = (
    await checkbox({
      message: `Configure the "${color.bold(keyName)}" key:`,
      choices: [
        new Separator('--- Permissions ---'),
        ...allPerms.map((perm, i) => ({
          name: `${desc[i][0]}     ${color.gray(desc[i][1])}`,
          value: perm,
          checked: permSelected(perm),
        })),
        new Separator('--- Authorized Apps ---'),
        ...apps.map(app => ({
          name: app.name,
          value: app.id,
          checked:
            app.id === CREATED_APPS
              ? permSelected(APP_CREATE_PERMISSION)
              : appSelected(app),
        })),
      ],
      instructions: false,
      pageSize: 1000,
      validate: stripDescriptionsIfValid,
    })
  ).map(
    // Transform the '(created apps)' appID back to "app:create" if present.
    value => (value === CREATED_APPS ? APP_CREATE_PERMISSION : value),
  );

  return {
    appIDs: selected.filter(value => !value.includes(':')),
    perms: selected.filter(value => value.includes(':')),
  };
}

function stripDescriptionsIfValid(
  items: readonly Item<string>[],
): boolean | string {
  let hasPermission = false;
  let hasApp = false;
  let hasAppCreate = false;
  let hasAppPublish = false;

  const selected = items as Choice<string>[]; // Only the selected items are validated.
  selected.forEach(choice => {
    switch (choice.value) {
      case CREATED_APPS:
        hasAppCreate = true;
        break;
      case APP_PUBLISH_PERMISSION:
        hasAppPublish = true;
        break;
    }
    if (choice.value.includes(':')) {
      hasPermission = true;
    } else {
      hasApp = true;
    }
  });
  if (!hasPermission) {
    return 'Please choose at least one permission.';
  }
  if (!hasApp) {
    return 'Please choose at least one app.';
  }
  if (hasAppCreate && !hasAppPublish) {
    return `${APP_PUBLISH_PERMISSION} is required to create new apps.`;
  }

  // Before proceeding, delete the `name` fields ('permission    description') so that
  // the resulting output lists the `value` fields ('permission' only).
  selected.forEach(choice => {
    if (choice.value.includes(':') || choice.value === CREATED_APPS) {
      delete choice.name;
    }
  });
  return true;
}

type AppInfo = {
  id: string;
  name: string;
  inAppDirectory: boolean;
};

async function getApps(
  firestore: Firestore,
  teamID: string,
): Promise<AppInfo[]> {
  const config = readAppConfig();
  const defaultAppID = config?.apps?.default?.appID;
  const q = query(
    collection(firestore, APP_COLLECTION).withConverter(appViewDataConverter),
    where('teamID', '==', teamID),
  );
  const apps = await getDocs(q);
  return apps.docs.map(doc => ({
    id: doc.id,
    name: doc.data().name,
    inAppDirectory: doc.id === defaultAppID,
  }));
}
