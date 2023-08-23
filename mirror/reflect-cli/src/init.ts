import {getFirestore, type Firestore} from './firebase.js';
import {CreateRequest, create} from 'mirror-protocol/src/app.js';
import {App, appDataConverter, appPath} from 'mirror-schema/src/app.js';
import {
  standardReleaseChannelSchema,
  STABLE_RELEASE_CHANNEL,
  CANARY_RELEASE_CHANNEL,
} from 'mirror-schema/src/server.js';
import {must} from 'shared/src/must.js';
import * as v from 'shared/src/valita.js';
import {readAppConfig, writeAppConfig} from './app-config.js';
import {authenticate} from './auth-config.js';
import {getExistingAppsForUser} from './get-existing-apps-for-user.js';
import {makeRequester} from './requester.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function initOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('name', {
      describe: 'The name of existing app to use',
      type: 'string',
    })
    .option('channel', {
      describe: 'Which channel to use',
      choices: [STABLE_RELEASE_CHANNEL, CANARY_RELEASE_CHANNEL],
      default: STABLE_RELEASE_CHANNEL,
    })
    .option('new', {
      describe: 'Create a new app',
      type: 'boolean',
    });
}

type InitHandlerArgs = YargvToInterface<ReturnType<typeof initOptions>>;

export async function initHandler(
  yargs: InitHandlerArgs,
  configDirPath?: string | undefined,
) {
  const user = await authenticate();

  const userID = user.uid;

  const {name, new: newApp} = yargs;
  const {channel} = yargs;
  v.assert(channel, standardReleaseChannelSchema);

  if (newApp) {
    if (name) {
      console.error('Cannot use --name with --new');
      process.exit(1);
    }
    await createNewApp(userID, channel, configDirPath);
    return;
  }

  const firestore = getFirestore();

  const appConfig = readAppConfig(configDirPath);
  if (!name && appConfig) {
    // Load the app from firebase to ensure it still exists.
    const app = await getApp(firestore, appConfig.appID);
    console.log(`Using app with name ${app.name}`);
    return;
  }

  // Check if user is already member of a team that has apps.
  const existingAppsForUser = await getExistingAppsForUser(firestore, userID);

  if (name) {
    // Check if the name flag is valid.
    const app = existingAppsForUser.find(app => app.name === name);
    if (!app) {
      console.error(`No app with name ${name} found.`);
      process.exit(1);
    }

    writeAppConfig({appID: app.appID}, configDirPath);
    console.log(`Using app with name ${app.name}`);
    return;
  }

  if (existingAppsForUser.length === 0) {
    // New app.
    console.log('User is not member of any team(s) that has apps.');
    console.log('Creating new app.');
    await createNewApp(userID, channel, configDirPath);
    return;
  }

  if (existingAppsForUser.length === 1) {
    // User is only member of one team with apps. Use that app.
    console.log('User is member of team with a single app. Using that app.');
    writeAppConfig({appID: existingAppsForUser[0].appID}, configDirPath);
    return;
  }

  // User is member of multiple teams with apps. Check if name flag is set
  // and present in list of apps.
  console.log('User is member of team(s) with multiple apps:');
  console.log('');

  for (const app of existingAppsForUser) {
    console.log(
      `  ${app.name} (appID: ${app.appID}, channel: ${app.serverReleaseChannel})`,
    );
  }
  console.log('');
  console.log('Please specify which app to use with --name flag.');
  process.exit(1);
}

async function createNewApp(
  userID: string,
  channel: 'canary' | 'stable',
  configDirPath?: string | undefined,
) {
  const data: CreateRequest = {
    requester: makeRequester(userID),
    serverReleaseChannel: channel,
  };

  const {appID, name: appName} = await create(data);
  writeAppConfig({appID}, configDirPath);
  console.log(`Created app ${appID} (${appName})`);
}

export function getApp(firestore: Firestore, appID: string): Promise<App> {
  const docRef = firestore.doc(appPath(appID)).withConverter(appDataConverter);

  return firestore.runTransaction(async txn => {
    const doc = await txn.get(docRef);
    if (!doc.exists) {
      throw new Error(`App with appID ${appID} does not exist`);
    }

    return must(doc.data());
  });
}
