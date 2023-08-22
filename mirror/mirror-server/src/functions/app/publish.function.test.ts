import {describe, expect, test, jest} from '@jest/globals';
import {fakeFirestore} from 'mirror-schema/src/test-helpers.js';
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
  defaultOptions,
  deploymentDataConverter,
} from 'mirror-schema/src/deployment.js';
import {appDataConverter} from 'mirror-schema/src/app.js';

describe('publish', () => {
  const request: PublishRequest = {
    requester: {
      userID: 'foo',
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    appID: 'fooApp',
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
    serverVersions: string[];
    expectedServerVersion?: string;
    requestAdditions?: Partial<PublishRequest>;
    errorCode?: FunctionsErrorCode;
  };
  const cases: Case[] = [
    {
      name: 'one server version',
      serverVersions: ['0.28.0'],
      expectedServerVersion: '0.28.0',
    },
    {
      name: 'multiple server candidates',
      serverVersions: ['0.28.0', '0.28.1', '0.29.0'],
      expectedServerVersion: '0.28.1',
    },
    {
      name: 'no matching server version',
      serverVersions: ['0.27.1', '0.29.0'], // Nothing matching "^0.28.0"
      errorCode: 'out-of-range',
    },
    {
      name: 'duplicate module names',
      serverVersions: ['0.28.0'],
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
      const firestore = fakeFirestore();

      await firestore.doc('users/foo').set({roles: {fooTeam: 'admin'}});
      await firestore.doc('apps/fooApp').withConverter(appDataConverter).set({
        name: 'foo-bar',
        cfID: '123',
        cfScriptName: 'foo-bar-script',
        serverReleaseChannel: 'stable',
        teamID: 'fooTeam',
        deploymentOptions: defaultOptions(),
      });
      for (const version of c.serverVersions) {
        await firestore.doc(`servers/${version}`).set({});
      }

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
          auth: {uid: 'foo'} as AuthData,
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
          .collection(`apps/fooApp/deployments`)
          .withConverter(deploymentDataConverter)
          .get();
        expect(deployments.size).toBe(1);
        const deployment = deployments.docs[0].data();
        expect(deployment).toMatchObject({
          requesterID: 'foo',
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
            hostname: 'foo-bar.reflect-server.net',
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
      }
    });
  }
});
