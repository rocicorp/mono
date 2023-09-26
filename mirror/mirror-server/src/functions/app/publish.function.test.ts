import {describe, expect, test, jest, beforeAll, afterAll} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import {getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import {
  FunctionsErrorCode,
  HttpsError,
  Request,
} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';
import {publish} from './publish.function.js';
import type {PublishRequest} from 'mirror-protocol/src/publish.js';
import type {Storage} from 'firebase-admin/storage';
import {
  appPath,
  defaultOptions,
  deploymentDataConverter,
} from 'mirror-schema/src/deployment.js';
import {appDataConverter} from 'mirror-schema/src/app.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {serverDataConverter, serverPath} from 'mirror-schema/src/server.js';
import {mockFunctionParamsAndSecrets} from '../../test-helpers.js';
import {
  DEFAULT_PROVIDER_ID,
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';

mockFunctionParamsAndSecrets();

describe('publish', () => {
  initializeApp({projectId: 'publish-function-test'});
  const firestore = getFirestore();
  const USER_ID = 'app-publish-test-user';
  const APP_ID = 'app-publish-test-app';
  const CF_ID = 'cf-abc';

  beforeAll(async () => {
    const batch = firestore.batch();
    batch.create(
      firestore.doc(userPath(USER_ID)).withConverter(userDataConverter),
      {
        email: 'foo@bar.com',
        roles: {fooTeam: 'admin'},
      },
    );
    batch.create(
      firestore
        .doc(providerPath(DEFAULT_PROVIDER_ID))
        .withConverter(providerDataConverter),
      {
        accountID: CF_ID,
        defaultZone: {
          zoneID: 'zone-id',
          zoneName: 'reflect-o-rama.net',
        },
        defaultMaxApps: 3,
        dispatchNamespace: 'prod',
      },
    );
    batch.create(
      firestore.doc(appPath(APP_ID)).withConverter(appDataConverter),
      {
        name: 'foo-bar',
        teamLabel: 'teamblue',
        provider: DEFAULT_PROVIDER_ID,
        cfID: 'deprecated',
        cfScriptName: 'foo-bar-script',
        serverReleaseChannel: 'stable',
        teamID: 'fooTeam',
        deploymentOptions: defaultOptions(),
      },
    );
    batch.create(
      firestore.doc(serverPath('0.28.0')).withConverter(serverDataConverter),
      {
        channels: ['canary', 'stable'],
        major: 0,
        minor: 28,
        patch: 0,
        modules: [],
      },
    );
    batch.create(
      firestore.doc(serverPath('0.28.1')).withConverter(serverDataConverter),
      {
        channels: ['canary'],
        major: 0,
        minor: 28,
        patch: 1,
        modules: [],
      },
    );
    batch.create(
      firestore.doc(serverPath('0.29.0')).withConverter(serverDataConverter),
      {
        channels: ['canary'],
        major: 0,
        minor: 29,
        patch: 0,
        modules: [],
      },
    );
    await batch.commit();
  });

  // Clean up test data from global emulator state.
  afterAll(async () => {
    const batch = firestore.batch();
    for (const path of [
      userPath(USER_ID),
      appPath(APP_ID),
      providerPath(DEFAULT_PROVIDER_ID),
      serverPath('0.28.0'),
      serverPath('0.28.1'),
      serverPath('0.29.0'),
    ]) {
      batch.delete(firestore.doc(path));
    }
    await batch.commit();
  });

  const request: PublishRequest = {
    requester: {
      userID: USER_ID,
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    appID: APP_ID,
    source: {
      name: 'index.js',
      content: 'console.log("hello world")',
    },
    sourcemap: {
      name: 'index.js.map',
      content: 'foo=bar',
    },
    serverVersionRange: '^0.28.0',
  };

  type Case = {
    name: string;
    serverReleaseChannel: string;
    expectedServerVersion?: string;
    requestAdditions?: Partial<PublishRequest>;
    errorCode?: FunctionsErrorCode;
  };
  const cases: Case[] = [
    {
      name: 'one server version',
      serverReleaseChannel: 'stable',
      expectedServerVersion: '0.28.0',
    },
    {
      name: 'multiple server candidates',
      serverReleaseChannel: 'canary',
      expectedServerVersion: '0.28.1',
    },
    {
      name: 'no matching server version',
      serverReleaseChannel: 'no-candidates',
      errorCode: 'out-of-range',
    },
    {
      name: 'duplicate module names',
      serverReleaseChannel: 'stable',
      requestAdditions: {
        sourcemap: {
          name: 'index.js', // Same as source file name
          content: 'foo=bar',
        },
      },
      errorCode: 'invalid-argument',
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await firestore
        .doc(appPath(APP_ID))
        .withConverter(appDataConverter)
        .update({serverReleaseChannel: c.serverReleaseChannel});

      const save = jest.fn();
      const storage = {
        bucket: (bucketName: string) => {
          expect(bucketName).toBe('modulez');
          return {
            file: (filename: string) => ({
              cloudStorageURI: {href: `gs://modulez/${filename}`},
              exists: () => [false],
              save,
            }),
          };
        },
      } as unknown as Storage;
      const publishFunction = https.onCall(
        publish(firestore, storage, 'modulez'),
      );

      let error: HttpsError | undefined = undefined;
      try {
        await publishFunction.run({
          auth: {uid: USER_ID} as AuthData,
          data: {
            ...request,
            ...c.requestAdditions,
          },
          rawRequest: null as unknown as Request,
        });
      } catch (e) {
        expect(e).toBeInstanceOf(HttpsError);
        error = e as HttpsError;
      }

      expect(error?.code).toBe(c.errorCode);
      if (!c.errorCode) {
        expect(save).toBeCalledTimes(2);

        const deployments = await firestore
          .collection(`apps/${APP_ID}/deployments`)
          .withConverter(deploymentDataConverter)
          .get();
        expect(deployments.size).toBe(1);
        const deployment = deployments.docs[0].data();
        expect(deployment).toMatchObject({
          requesterID: USER_ID,
          type: 'USER_UPLOAD',
          spec: {
            appModules: [
              {
                name: 'index.js',
                type: 'esm',
                url: 'gs://modulez/e7fb2f4978d27e4f9e23fe22cea2bb3da1632fabb50362e2963c6870a6f1a5',
              },
              {
                name: 'index.js.map',
                type: 'text',
                url: 'gs://modulez/3ba8907e7a252327488df390ed517c45b96dead03360019bdca710d1d3f88a',
              },
            ],
            serverVersion: c.expectedServerVersion,
            serverVersionRange: request.serverVersionRange,
            hostname: 'foo-bar-teamblue.reflect-o-rama.net',
            options: {
              vars: {
                /* eslint-disable @typescript-eslint/naming-convention */
                DISABLE_LOG_FILTERING: 'false',
                LOG_LEVEL: 'info',
                /* eslint-enable @typescript-eslint/naming-convention */
              },
            },
          },
          status: 'REQUESTED',
        });
        expect(deployment).toHaveProperty('requestTime');

        // Cleanup.
        await deployments.docs[0].ref.delete();
      }
    });
  }
});
