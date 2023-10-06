import {ensureUser} from 'mirror-protocol/src/user.js';
import {authenticate} from './auth-config.js';
import {makeRequester} from './requester.js';
import {getFirestore} from './firebase.js';
import color from 'picocolors';
import {appPath} from 'mirror-schema/src/app.js';
import {readAppConfig} from './app-config.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

interface AppData {
  name?: string;
  runningDeployment?: {
    status?: string;
    spec?: {
      hostname?: string;
      serverVersion?: string;
    };
  };
}

export async function statusHandler(
  yargs: YargvToInterface<CommonYargsArgv>,
): Promise<void> {
  const {userID} = await authenticate(yargs);
  const data = {requester: makeRequester(userID)};
  await ensureUser(data);

  const firestore = getFirestore();
  const config = readAppConfig();
  const defaultAppID = config?.apps?.default?.appID;

  if (!defaultAppID) {
    return displayStatus();
  }

  const appData: AppData | undefined = (
    await firestore.doc(appPath(defaultAppID)).get()
  ).data();

  displayStatus(appData);
}

function displayStatus(appData?: AppData): void {
  const getStatusText = (value: string | undefined, label: string): string =>
    color.green(`${label}: `) +
    color.reset(value ? value : color.red('Unknown'));

  console.log(`-------------------------------------------------`);
  console.log(getStatusText(appData?.name, 'App'));

  if (appData?.name) {
    console.log(
      getStatusText(appData.runningDeployment?.status + '🏃', 'Status'),
    );
    console.log(
      getStatusText(appData.runningDeployment?.spec?.hostname, 'Hostname'),
    );
    console.log(
      getStatusText(
        appData.runningDeployment?.spec?.serverVersion,
        'Server Version',
      ),
    );
  }

  console.log(`-------------------------------------------------`);
}
