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

export function appListOptions(yargs: CommonYargsArgv) {
  return yargs.option('output', {
    describe: 'Output the result in a specified format',
    type: 'string',
    requiresArg: true,
    choices: ['json'],
  });
}

type AppListOptionArgs = YargvToInterface<ReturnType<typeof appListOptions>>;

export async function appListHandler(
  yargs: AppListOptionArgs,
  authContext: AuthContext,
): Promise<void> {
  const {output} = yargs;
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
    console.log('No apps found.');
    return;
  }

  if (output === 'json') {
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
    }
    console.log(JSON.stringify(appList, null, 2));
  } else {
    for (const doc of apps.docs) {
      const appView = doc.data();
      displayApp(doc.id, appView);
    }
  }
}

function displayApp(appID?: string, appView?: AppView): void {
  const getAppText = (label: string, value: string | undefined): string =>
    color.green(`${label}: `) +
    color.reset(value ? value : color.red('Unknown'));

  console.log(`-------------------------------------------------`);
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
    console.log(
      getAppText(label + pad.repeat(maxLabelLen - label.length), value),
    );
  }
  console.log(`-------------------------------------------------`);
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
