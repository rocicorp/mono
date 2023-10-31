/* eslint-disable @typescript-eslint/naming-convention */
import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import {Timestamp, getFirestore} from 'firebase-admin/firestore';
import {appDataConverter} from 'mirror-schema/src/app.js';
import {
  appPath,
  deploymentDataConverter,
  deploymentPath,
} from 'mirror-schema/src/deployment.js';
import {
  DEFAULT_PROVIDER_ID,
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';
import {serverDataConverter, serverPath} from 'mirror-schema/src/server.js';
import {must} from 'shared/src/must.js';
import {TestSecrets} from '../../secrets/test-utils.js';
import {mockFunctionParamsAndSecrets} from '../../test-helpers.js';
import {getAppSecrets} from '../app/secrets.js';
import {
  checkAppsInChannels,
  getAffectedChannels,
} from './auto-deploy.function.js';

test('getAffectedChannels', () => {
  expect(getAffectedChannels(['canary'], ['stable'])).toEqual([
    'canary',
    'stable',
  ]);
  expect(getAffectedChannels(['canary', 'stable'], ['stable'])).toEqual([
    'canary',
  ]);
  expect(getAffectedChannels(['canary'], ['canary', 'stable'])).toEqual([
    'stable',
  ]);
  expect(getAffectedChannels(['canary', 'beta'], ['beta', 'stable'])).toEqual([
    'canary',
    'stable',
  ]);
  expect(
    getAffectedChannels(['canary', 'beta'], ['canary', 'beta', 'stable']),
  ).toEqual(['stable']);
});

describe('server auto-deploy', () => {
  initializeApp({projectId: 'server-auto-deploy-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'server-auto-deploy-test-app-';
  const SERVER_VERSION_1 = '0.209.1';
  const SERVER_VERSION_2 = '0.209.2';
  const CLOUDFLARE_ACCOUNT_ID = 'cf-abc';

  const appDocs = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map(i =>
    firestore.doc(appPath(`${APP_ID}${i}`)).withConverter(appDataConverter),
  );

  beforeEach(async () => {
    mockFunctionParamsAndSecrets();

    const batch = firestore.batch();
    const {hashes} = await getAppSecrets(new TestSecrets(), {});
    batch.create(
      firestore
        .doc(serverPath(SERVER_VERSION_1))
        .withConverter(serverDataConverter),
      {
        major: 0,
        minor: 209,
        patch: 1,
        modules: [],
        channels: ['test-stable', 'test-canary'],
      },
    );
    batch.create(
      firestore
        .doc(serverPath(SERVER_VERSION_2))
        .withConverter(serverDataConverter),
      {
        major: 0,
        minor: 209,
        patch: 2,
        modules: [],
        channels: ['test-canary'],
      },
    );
    batch.create(
      firestore
        .doc(providerPath(DEFAULT_PROVIDER_ID))
        .withConverter(providerDataConverter),
      {
        accountID: CLOUDFLARE_ACCOUNT_ID,
        defaultMaxApps: 3,
        defaultZone: {
          zoneID: 'zone-id',
          zoneName: 'reflect-o-rama.net',
        },
        dispatchNamespace: 'prod',
      },
    );

    appDocs.forEach((appDoc, i) => {
      batch.create(appDoc, {
        cfID: 'deprecated',
        provider: DEFAULT_PROVIDER_ID,
        cfScriptName: 'bar',
        teamID: 'baz',
        teamLabel: 'boom',
        name: 'boo',
        deploymentOptions: {
          vars: {
            DISABLE: 'false',
            DISABLE_LOG_FILTERING: 'false',
            LOG_LEVEL: 'info',
          },
        },
        secrets: {},
        serverReleaseChannel: i % 2 === 0 ? 'test-stable' : 'test-canary',

        runningDeployment: {
          deploymentID: 'do',
          requesterID: 'user',
          type: 'USER_UPLOAD',
          status: 'RUNNING',
          requestTime: Timestamp.now(),
          spec: {
            appModules: [],
            serverVersionRange: '^0.209.0',
            serverVersion: '0.209.0',
            hostname: 'boo-boom.reflect-o-rama.net',
            options: {
              vars: {
                DISABLE: 'false',
                DISABLE_LOG_FILTERING: 'false',
                LOG_LEVEL: 'info',
              },
            },
            hashesOfSecrets: hashes,
          },
        },
      });
    });
    await batch.commit();
  });

  afterEach(async () => {
    const batch = firestore.batch();
    for (const appDoc of appDocs) {
      batch.delete(appDoc);
    }
    for (const path of [
      serverPath(SERVER_VERSION_1),
      serverPath(SERVER_VERSION_2),
      providerPath(DEFAULT_PROVIDER_ID),
    ]) {
      batch.delete(firestore.doc(path));
    }
    await batch.commit();
  });

  type Case = {
    name: string;
    channels: string[];
    expectQueued: {[app: number]: string};
  };

  const cases: Case[] = [
    {
      name: 'no channels',
      channels: [],
      expectQueued: {},
    },
    {
      name: 'test-canary',
      channels: ['test-canary'],
      expectQueued: {
        1: SERVER_VERSION_2,
        3: SERVER_VERSION_2,
        5: SERVER_VERSION_2,
        7: SERVER_VERSION_2,
        9: SERVER_VERSION_2,
      },
    },
    {
      name: 'test-stable',
      channels: ['test-stable'],
      expectQueued: {
        0: SERVER_VERSION_1,
        2: SERVER_VERSION_1,
        4: SERVER_VERSION_1,
        6: SERVER_VERSION_1,
        8: SERVER_VERSION_1,
      },
    },
    {
      name: 'multiple channels, multiple versions',
      channels: ['test-canary', 'test-stable'],
      expectQueued: {
        0: SERVER_VERSION_1,
        1: SERVER_VERSION_2,
        2: SERVER_VERSION_1,
        3: SERVER_VERSION_2,
        4: SERVER_VERSION_1,
        5: SERVER_VERSION_2,
        6: SERVER_VERSION_1,
        7: SERVER_VERSION_2,
        8: SERVER_VERSION_1,
        9: SERVER_VERSION_2,
      },
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await checkAppsInChannels(firestore, new TestSecrets(), c.channels, 3);

      const apps = await Promise.all(appDocs.map(doc => doc.get()));
      apps.forEach(async (app, i) => {
        const expectedVersion = c.expectQueued[i];
        if (!expectedVersion) {
          expect(app.data()?.queuedDeploymentIDs).toBeUndefined;
        } else {
          expect(app.data()?.queuedDeploymentIDs).toHaveLength(1);
          const deploymentID = must(app.data()?.queuedDeploymentIDs?.[0]);
          const deployment = await firestore
            .doc(deploymentPath(app.id, deploymentID))
            .withConverter(deploymentDataConverter)
            .get();
          expect(deployment.data()?.spec.serverVersion).toBe(expectedVersion);
        }
      });
    });
  }
});
