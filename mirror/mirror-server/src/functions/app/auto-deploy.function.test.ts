/* eslint-disable @typescript-eslint/naming-convention */
import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import {FieldValue, Timestamp, getFirestore} from 'firebase-admin/firestore';
import {appDataConverter} from 'mirror-schema/src/app.js';
import {
  DeploymentSpec,
  DeploymentType,
  appPath,
  deploymentDataConverter,
  deploymentPath,
  deploymentsCollection,
} from 'mirror-schema/src/deployment.js';
import {DEFAULT_ENV, envDataConverter, envPath} from 'mirror-schema/src/env.js';
import {
  DEFAULT_PROVIDER_ID,
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';
import {serverDataConverter, serverPath} from 'mirror-schema/src/server.js';
import {must} from 'shared/src/must.js';
import {
  MAX_AUTO_DEPLOYMENTS_PER_MINUTE,
  MIRROR_SERVER_REQUESTER_ID,
  checkForAutoDeployment,
} from './auto-deploy.function.js';

describe('app.auto-deploy', () => {
  initializeApp({projectId: 'auto-deploy-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'auto-deploy-test-app-id';
  const SERVER_VERSION_1 = '0.28.0';
  const SERVER_VERSION_2 = '0.28.2';
  const CLOUDFLARE_ACCOUNT_ID = 'foo-cloudflare-account';
  const ENV_UPDATE_TIME = Timestamp.fromDate(new Date(2023, 1, 6));

  beforeEach(async () => {
    const batch = firestore.batch();
    batch.create(
      firestore
        .doc(serverPath(SERVER_VERSION_1))
        .withConverter(serverDataConverter),
      {
        major: 0,
        minor: 28,
        patch: 0,
        modules: [],
        channels: ['stable', 'canary'],
      },
    );
    batch.create(
      firestore
        .doc(serverPath(SERVER_VERSION_2))
        .withConverter(serverDataConverter),
      {
        major: 0,
        minor: 28,
        patch: 2,
        modules: [],
        channels: ['canary'],
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
    batch.create(
      firestore.doc(appPath(APP_ID)).withConverter(appDataConverter),
      {
        cfID: 'deprecated',
        provider: DEFAULT_PROVIDER_ID,
        cfScriptName: 'bar',
        teamID: 'baz',
        name: 'boo',
        teamLabel: 'yah',
        serverReleaseChannel: 'stable',

        runningDeployment: {
          deploymentID: 'do',
          requesterID: 'user',
          type: 'USER_UPLOAD',
          status: 'RUNNING',
          requestTime: Timestamp.now(),
          spec: {
            appModules: [],
            serverVersionRange: '^0.28.0',
            serverVersion: SERVER_VERSION_1,
            hostname: 'boo-yah.reflect-o-rama.net',
            envUpdateTime: ENV_UPDATE_TIME,
          },
        },

        envUpdateTime: ENV_UPDATE_TIME,
      },
    );
    batch.create(
      firestore
        .doc(envPath(APP_ID, DEFAULT_ENV))
        .withConverter(envDataConverter),
      {
        deploymentOptions: {
          vars: {
            DISABLE: 'false',
            DISABLE_LOG_FILTERING: 'false',
            LOG_LEVEL: 'info',
          },
        },
        secrets: {},
      },
    );
    await batch.commit();
  });

  afterEach(async () => {
    const batch = firestore.batch();
    const deployments = await firestore
      .collection(deploymentsCollection(APP_ID))
      .listDocuments();
    for (const d of deployments) {
      batch.delete(d);
    }
    for (const path of [
      envPath(APP_ID, DEFAULT_ENV),
      appPath(APP_ID),
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
    prep: () => Promise<void>;
    expectedType?: DeploymentType;
    expectedSpec?: Partial<DeploymentSpec>;
  };

  const cases: Case[] = [
    {
      name: 'no changes',
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      prep: async () => {},
    },
    {
      name: 'server update',
      prep: async () => {
        await firestore.doc(serverPath(SERVER_VERSION_2)).update({
          channels: FieldValue.arrayUnion('stable'),
        });
      },
      expectedType: 'SERVER_UPDATE',
      expectedSpec: {
        serverVersion: SERVER_VERSION_2,
      },
    },
    {
      name: 'hostname update',
      prep: async () => {
        await firestore.doc(appPath(APP_ID)).update({
          name: 'bonk',
        });
      },
      expectedType: 'HOSTNAME_UPDATE',
      expectedSpec: {
        hostname: 'bonk-yah.reflect-o-rama.net',
      },
    },
    {
      name: 'env update',
      prep: async () => {
        await firestore
          .doc(appPath(APP_ID))
          .withConverter(appDataConverter)
          .update({envUpdateTime: Timestamp.fromDate(new Date(2023, 1, 7))});
      },
      expectedType: 'ENV_UPDATE',
      expectedSpec: {
        envUpdateTime: Timestamp.fromDate(new Date(2023, 1, 7)),
      },
    },
    {
      name: 'forced redeployment',
      prep: async () => {
        await firestore.doc(appPath(APP_ID)).update({
          forceRedeployment: true,
        });
      },
      expectedType: 'MAINTENANCE_UPDATE',
    },
  ];

  for (const c of cases) {
    test(`${c.name}: app.auto-deploy`, async () => {
      await c.prep();

      const appDoc = firestore
        .doc(appPath(APP_ID))
        .withConverter(appDataConverter);

      const before = await appDoc.get();
      const beforeApp = must(before.data());
      const lastUpdateTime = must(before.updateTime);

      await checkForAutoDeployment(
        firestore,
        APP_ID,
        beforeApp,
        lastUpdateTime,
      );

      const afterApp = must((await appDoc.get()).data());
      if (!c.expectedType) {
        expect(afterApp.queuedDeploymentIDs).toBeUndefined;
      } else {
        expect(afterApp.queuedDeploymentIDs).toHaveLength(1);
        expect(afterApp.forceRedeployment).toBeUndefined;
        const queuedDeploymentID = must(afterApp.queuedDeploymentIDs)[0];

        const deployment = await firestore
          .doc(deploymentPath(APP_ID, queuedDeploymentID))
          .withConverter(deploymentDataConverter)
          .get();
        expect(deployment.data()?.type).toEqual(c.expectedType);
        expect(deployment.data()?.spec).toEqual({
          ...beforeApp.runningDeployment?.spec,
          ...c.expectedSpec,
        });
      }
    });
  }

  for (const c of cases) {
    if (!c.expectedType) {
      continue;
    }

    test(`no ${c.name} if deployments are queued`, async () => {
      await c.prep();

      const appDoc = firestore
        .doc(appPath(APP_ID))
        .withConverter(appDataConverter);

      await appDoc.update({
        queuedDeploymentIDs: FieldValue.arrayUnion('12345'),
      });

      const before = await appDoc.get();
      const beforeApp = must(before.data());
      const lastUpdateTime = must(before.updateTime);

      await checkForAutoDeployment(
        firestore,
        APP_ID,
        beforeApp,
        lastUpdateTime,
      );
      const result = await appDoc.get();
      expect(result.data()?.queuedDeploymentIDs).toEqual(['12345']);
    });
  }

  for (const c of cases) {
    if (!c.expectedType) {
      continue;
    }

    test(`no ${c.name} if too many recent deployments`, async () => {
      await c.prep();

      const appDoc = firestore
        .doc(appPath(APP_ID))
        .withConverter(appDataConverter);

      const batch = firestore.batch();
      for (let i = 0; i < MAX_AUTO_DEPLOYMENTS_PER_MINUTE; i++) {
        batch.create(
          firestore
            .doc(deploymentPath(APP_ID, `${i}`))
            .withConverter(deploymentDataConverter),
          {
            deploymentID: `${i}`,
            requesterID: MIRROR_SERVER_REQUESTER_ID,
            type: 'OPTIONS_UPDATE',
            requestTime: Timestamp.fromMillis(Date.now() - 10 * i),
            status: 'STOPPED',
            spec: {
              appModules: [],
              serverVersionRange: '^0.28.0',
              serverVersion: SERVER_VERSION_1,
              envUpdateTime: Timestamp.now(),
              hostname: 'boo-yah.reflect-o-rama.net',
            },
          },
        );
      }

      const before = await appDoc.get();
      const beforeApp = must(before.data());
      const lastUpdateTime = must(before.updateTime);

      await checkForAutoDeployment(
        firestore,
        APP_ID,
        beforeApp,
        lastUpdateTime,
      );
      const result = await appDoc.get();
      expect(result.data()?.queuedDeploymentIDs).toBeUndefined;
    });
  }
});
