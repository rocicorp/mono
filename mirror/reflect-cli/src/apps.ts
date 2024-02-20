import {
  collection,
  getDocs,
  getFirestore,
  query,
  where,
} from 'firebase/firestore';
import {
  AppView,
  appViewDataConverter,
  APP_COLLECTION,
} from 'mirror-schema/src/external/app.js';

import type {DeploymentView} from 'mirror-schema/src/external/deployment.js';
import color from 'picocolors';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import type {AuthContext} from './handler.js';
import {getSingleTeam} from './teams.js';
import {getLogger} from './logger.js';

export function appListOptions(yargs: CommonYargsArgv) {
  return yargs.option('output', {
    describe: 'Output the result in a specified format',
    type: 'string',
    requiresArg: true,
    choices: ['json', 'text'],
    default: 'text',
  });
}

type AppListOptionArgs = YargvToInterface<ReturnType<typeof appListOptions>>;

export async function appListHandler(
  _yargs: AppListOptionArgs,
  authContext: AuthContext,
): Promise<void> {
  const firestore = getFirestore();
  const teamID = await getSingleTeam(
    firestore,
    authContext.user.userID,
    'admin',
  );
  const q = query(
    collection(firestore, APP_COLLECTION).withConverter(appViewDataConverter),
    where('teamID', '==', teamID),
  );

  const apps = await getDocs(q);
  if (apps.size === 0) {
    getLogger().log('No apps found.');
    return;
  }

  const appList = [];
  for (const doc of apps.docs) {
    const appView = doc.data();
    appList.push({
      name: appView?.name,
      id: doc.id,
      status: getDeploymentStatus(appView?.runningDeployment),
      hostname: appView?.runningDeployment?.spec.hostname,
      serverVersion: appView?.runningDeployment?.spec.serverVersion,
    });
    displayApp(doc.id, appView);
  }
  getLogger().json(appList);

  function displayApp(appID?: string, appView?: AppView): void {
    const getAppText = (label: string, value: string | undefined): string =>
      color.green(`${label}: `) +
      color.reset(value ? value : color.red('Unknown'));

    getLogger().log(`-------------------------------------------------`);
    const lines: [string, string | undefined][] = appView?.name
      ? [
          ['App', appView?.name],
          ['ID', appID],
          ['Status', getDeploymentStatus(appView?.runningDeployment)],
          ['Hostname', appView?.runningDeployment?.spec.hostname],
          ['Server Version', appView?.runningDeployment?.spec.serverVersion],
        ]
      : [['App', undefined]];

    const maxLabelLen = Math.max(...lines.map(l => l[0].length));
    const pad = ' ';
    for (const [label, value] of lines) {
      getLogger().log(
        getAppText(label + pad.repeat(maxLabelLen - label.length), value),
      );
    }
    getLogger().log(`-------------------------------------------------`);
  }

  function getDeploymentStatus(deployment?: DeploymentView): string {
    switch (deployment?.status) {
      case 'RUNNING':
        return `${deployment?.status}`;
      case undefined:
        return 'Awaiting first publish';
    }
    return deployment?.statusMessage
      ? `${deployment?.status}: ${deployment?.statusMessage}`
      : `${deployment?.status}`;
  }
}
