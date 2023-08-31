import {getFirestore, type Firestore} from './firebase.js';
import {createApp} from 'mirror-protocol/src/app.js';
import {ensureTeam} from 'mirror-protocol/src/team.js';

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
      describe: 'The name of app to use (or create with --new)',
      type: 'string',
      demandOption: true,
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

  const {name: appName, new: newApp, channel} = yargs;
  v.assert(channel, standardReleaseChannelSchema);

  if (newApp) {
    const defaultTeamName = user.additionalUserInfo?.username;
    if (!defaultTeamName) {
      throw new Error('Could not determine github username from oauth');
    }
    await createNewApp(
      userID,
      defaultTeamName,
      appName,
      channel,
      configDirPath,
    );
    return;
  }

  const firestore = getFirestore();

  const appConfig = readAppConfig(configDirPath);
  if (appConfig) {
    // Load the app from firebase to ensure it still exists.
    const app = await getApp(firestore, appConfig.appID);
    console.log(`Already configured to use app "${app.name}"`);
    return;
  }

  const existingAppsForUser = await getExistingAppsForUser(firestore, userID);
  const app = existingAppsForUser.find(app => app.name === appName);
  if (app) {
    writeAppConfig({appID: app.appID}, configDirPath);
    console.log(`Ready to use app "${app.name}"`);
    return;
  }

  console.log('');
  console.error(
    `Did not find an app named ${appName}. Please specify --new to create one.`,
  );
  process.exit(1);
}

async function createNewApp(
  userID: string,
  defaultTeamName: string,
  appName: string,
  channel: 'canary' | 'stable',
  configDirPath?: string | undefined,
) {
  const requester = makeRequester(userID);
  const {teamID} = await ensureTeam({
    requester,
    name: defaultTeamName,
  });
  const {appID} = await createApp({
    requester,
    teamID,
    name: appName,
    serverReleaseChannel: channel,
  });
  writeAppConfig({appID}, configDirPath);
  console.log(`Created app "${name}" (${appID})`);
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
