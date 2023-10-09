import {ensureUser} from 'mirror-protocol/src/user.js';
import {authenticate} from './auth-config.js';
import {makeRequester} from './requester.js';
import {getFirestore} from './firebase.js';
import color from 'picocolors';
import {appPath} from 'mirror-schema/src/app.js';
import {readAppConfig} from './app-config.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

interface AppData {
  appID?: string | undefined;
  name?: string | undefined;
  runningDeployment?: {
    status?: string | undefined;
    spec?: {
      hostname?: string | undefined;
      serverVersion?: string | undefined;
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

  if (appData) {
    appData.appID = defaultAppID;
  }

  displayStatus(appData);
}

function displayStatus(appData?: AppData): void {
  const getStatusText = (label: string, value: string | undefined): string =>
    color.green(`${label}: `) +
    color.reset(value ? value : color.red('Unknown'));

  console.log(`-------------------------------------------------`);
  console.log(getStatusText('App', appData?.name));

  if (appData?.name) {
    console.log(getStatusText('ID', appData?.appID));
    console.log(
      getStatusText('Status', appData.runningDeployment?.status + '🏃'),
    );
    console.log(
      getStatusText('Hostname', appData.runningDeployment?.spec?.hostname),
    );
    console.log(
      getStatusText(
        'Server Version',
        appData.runningDeployment?.spec?.serverVersion,
      ),
    );
  }

  console.log(`-------------------------------------------------`);
}
