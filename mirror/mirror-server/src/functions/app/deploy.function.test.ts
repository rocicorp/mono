import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import {resolver} from '@rocicorp/resolver';
import {initializeApp} from 'firebase-admin/app';
import {FieldValue, Timestamp, getFirestore} from 'firebase-admin/firestore';
import type {Storage} from 'firebase-admin/storage';
import {appDataConverter, type ScriptRef} from 'mirror-schema/src/app.js';
import {encryptUtf8} from 'mirror-schema/src/crypto.js';
import {
  Deployment,
  DeploymentStatus,
  DeploymentType,
  appPath,
  deploymentDataConverter,
  deploymentPath,
  deploymentsCollection,
} from 'mirror-schema/src/deployment.js';
import {
  DEFAULT_ENV,
  ENCRYPTION_KEY_SECRET_NAME,
  envDataConverter,
  envPath,
} from 'mirror-schema/src/env.js';
import {
  DEFAULT_PROVIDER_ID,
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';
import {serverDataConverter, serverPath} from 'mirror-schema/src/server.js';
import {appNameIndexPath, teamPath} from 'mirror-schema/src/team.js';
import {
  getApp,
  getTeam,
  setApp,
  setAppName,
  setEnv,
  setTeam,
} from 'mirror-schema/src/test-helpers.js';
import {must} from 'shared/src/must.js';
import {Queue} from 'shared/src/queue.js';
import {sleep} from 'shared/src/sleep.js';
import type {ScriptHandler} from '../../cloudflare/script-handler.js';
import {TestSecrets} from '../../secrets/test-utils.js';
import {mockFunctionParamsAndSecrets} from '../../test-helpers.js';
import {MIN_WFP_VERSION} from './create.function.js';
import {
  earlierDeployments,
  requestDeployment,
  runDeployment,
} from './deploy.function.js';

mockFunctionParamsAndSecrets();

describe('deploy', () => {
  initializeApp({projectId: 'deploy-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'deploy-test-app-id';
  const APP_NAME = 'my-app';
  const TEAM_ID = 'my-team';
  const SERVER_VERSION = '0.35.0';
  const WFP_SERVER_VERSION = MIN_WFP_VERSION.raw;
  const WFP_SERVER_VERSION_PRE_RELEASE_TAG = `${WFP_SERVER_VERSION}-canary.0`;
  const CLOUDFLARE_ACCOUNT_ID = 'foo-cloudflare-account';
  const NAMESPACE = 'prod';
  const SCRIPT_NAME = 'foo-bar-baz';
  const ENV_UPDATE_TIME = Timestamp.fromDate(new Date(2023, 5, 1));

  const noopScriptHandler: ScriptHandler = {
    async *publish(): AsyncGenerator<string> {},
    async delete(): Promise<void> {},
  };

  function testSecrets() {
    return new TestSecrets([
      `${DEFAULT_PROVIDER_ID}_api_token`,
      'latest',
      'api-token',
    ]);
  }

  beforeAll(async () => {
    const batch = firestore.batch();
    batch.create(
      firestore
        .doc(serverPath(SERVER_VERSION))
        .withConverter(serverDataConverter),
      {
        major: 0,
        minor: 35,
        patch: 0,
        modules: [],
        channels: ['stable'],
      },
    );
    batch.create(
      firestore
        .doc(serverPath(WFP_SERVER_VERSION))
        .withConverter(serverDataConverter),
      {
        major: MIN_WFP_VERSION.major,
        minor: MIN_WFP_VERSION.minor,
        patch: MIN_WFP_VERSION.patch,
        modules: [],
        channels: ['stable'],
      },
    );
    batch.create(
      firestore
        .doc(serverPath(WFP_SERVER_VERSION_PRE_RELEASE_TAG))
        .withConverter(serverDataConverter),
      {
        major: MIN_WFP_VERSION.major,
        minor: MIN_WFP_VERSION.minor,
        patch: MIN_WFP_VERSION.patch,
        modules: [],
        channels: ['stable'],
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
        dispatchNamespace: NAMESPACE,
      },
    );
    await batch.commit();
  });

  afterAll(async () => {
    const batch = firestore.batch();
    batch.delete(firestore.doc(serverPath(SERVER_VERSION)));
    batch.delete(firestore.doc(serverPath(WFP_SERVER_VERSION)));
    batch.delete(firestore.doc(serverPath(WFP_SERVER_VERSION_PRE_RELEASE_TAG)));
    batch.delete(firestore.doc(providerPath(DEFAULT_PROVIDER_ID)));
    await batch.commit();
  });

  beforeEach(async () => {
    await Promise.all([
      setApp(firestore, APP_ID, {
        teamID: TEAM_ID,
        name: APP_NAME,
        cfScriptName: SCRIPT_NAME,
      }),
      setTeam(firestore, TEAM_ID, {numApps: 1}),
      setAppName(firestore, TEAM_ID, APP_ID, APP_NAME),
      setEnv(firestore, APP_ID, {}),
    ]);
  });

  afterEach(async () => {
    // Clean up global emulator data.
    const firestore = getFirestore();
    const batch = firestore.batch();
    const deployments = await firestore
      .collection(deploymentsCollection(APP_ID))
      .listDocuments();
    for (const d of deployments) {
      batch.delete(d);
    }
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(teamPath(TEAM_ID)));
    batch.delete(firestore.doc(appNameIndexPath(TEAM_ID, APP_NAME)));
    batch.delete(firestore.doc(envPath(APP_ID, DEFAULT_ENV)));
    await batch.commit();
  });

  async function writeTestDeployment(
    deploymentID: string,
    status: DeploymentStatus,
  ): Promise<Deployment> {
    const deployment: Deployment = {
      deploymentID,
      requesterID: 'foo',
      type: 'USER_UPLOAD',
      spec: {
        appModules: [],
        hostname: 'boo',
        serverVersion: SERVER_VERSION,
        serverVersionRange: '1',
        envUpdateTime: ENV_UPDATE_TIME,
      },
      status,
      requestTime: Timestamp.now(),
    };
    await firestore
      .doc(deploymentPath(APP_ID, deploymentID))
      .withConverter(deploymentDataConverter)
      .create(deployment);
    return deployment;
  }

  async function requestTestDeployment(
    type: DeploymentType = 'USER_UPLOAD',
    serverVersion = SERVER_VERSION,
  ): Promise<string> {
    const deploymentPath = await requestDeployment(firestore, APP_ID, {
      requesterID: 'foo',
      type,
      spec: {
        appModules: [],
        hostname: 'boo',
        serverVersion,
        serverVersionRange: `^${serverVersion}`,
        envUpdateTime: ENV_UPDATE_TIME,
      },
    });
    return firestore.doc(deploymentPath).id;
  }

  async function getDeployment(id: string): Promise<Deployment> {
    const doc = firestore
      .doc(deploymentPath(APP_ID, id))
      .withConverter(deploymentDataConverter)
      .get();
    return must((await doc).data());
  }

  test('requestDeployment', async () => {
    const id1 = await requestTestDeployment();
    expect((await getApp(firestore, APP_ID)).queuedDeploymentIDs).toEqual([
      id1,
    ]);

    const id2 = await requestTestDeployment();
    expect((await getApp(firestore, APP_ID)).queuedDeploymentIDs).toEqual([
      id1,
      id2,
    ]);
  });

  test('deploys app secrets', async () => {
    const testSecrets = new TestSecrets(
      [`${DEFAULT_PROVIDER_ID}_api_token`, 'latest', 'api-token'],
      [ENCRYPTION_KEY_SECRET_NAME, '2', TestSecrets.TEST_KEY],
    );
    const encryptedSecret = encryptUtf8(
      'this is the decrypted app secret',
      Buffer.from(TestSecrets.TEST_KEY, 'base64url'),
      {version: '2'},
    );
    const envDocRef = firestore
      .doc(envPath(APP_ID, DEFAULT_ENV))
      .withConverter(envDataConverter);

    await envDocRef.update({secrets: {['MY_APP_SECRET']: encryptedSecret}});
    const envUpdateTime = must((await envDocRef.get()).updateTime);

    let deployedSecrets;

    const deploymentID = await requestTestDeployment();
    await runDeployment(
      firestore,
      null as unknown as Storage,
      testSecrets,
      APP_ID,
      deploymentID,
      {
        // eslint-disable-next-line require-yield
        async *publish(_storage, _app, _team, _hostname, _options, secrets) {
          deployedSecrets = secrets;
        },
        async delete(): Promise<void> {},
      },
    );

    expect(deployedSecrets).toEqual({
      ['MY_APP_SECRET']: 'this is the decrypted app secret',
      ['DATADOG_LOGS_API_KEY']: 'default-DATADOG_LOGS_API_KEY',
      ['DATADOG_METRICS_API_KEY']: 'default-DATADOG_METRICS_API_KEY',
    });

    const app = await getApp(firestore, APP_ID);
    expect(app.runningDeployment?.spec.envUpdateTime).toEqual(envUpdateTime);
    expect(app.envUpdateTime).toEqual(envUpdateTime);
  });

  test('state tracking: success', async () => {
    const {promise: isPublishing, resolve: publishing} = resolver<void>();
    const deploymentUpdates = new Queue<string>();

    const deploymentID = await requestTestDeployment();
    let deployment = await getDeployment(deploymentID);
    expect(deployment.status).toBe('REQUESTED');

    let app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([deploymentID]);
    expect(app.runningDeployment).toBeUndefined;

    const deploymentFinished = runDeployment(
      firestore,
      null as unknown as Storage,
      testSecrets(),
      APP_ID,
      deploymentID,
      {
        async *publish() {
          publishing();
          for (;;) {
            const update = await deploymentUpdates.dequeue();
            if (!update) {
              break;
            }
            yield update;
          }
        },
        async delete(): Promise<void> {},
      },
    );
    await isPublishing;

    deployment = await getDeployment(deploymentID);
    expect(deployment.status).toBe('DEPLOYING');
    expect(deployment.statusMessage).toBeUndefined;
    app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([deploymentID]);
    expect(app.runningDeployment).toBeUndefined;

    // Queue up another deployment while the first is publishing.
    const nextDeploymentID = await requestTestDeployment();
    app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([deploymentID, nextDeploymentID]);
    expect(app.runningDeployment).toBeUndefined;

    for (const update of ['deploying yo!', 'still deploying']) {
      void deploymentUpdates.enqueue(update);
      await sleep(200); // We *could* use a snapshot listener to wait for updates but that's a lot more code.
      deployment = await getDeployment(deploymentID);
      expect(deployment.status).toBe('DEPLOYING');
      expect(deployment.statusMessage).toBe(update);
    }
    void deploymentUpdates.enqueue('');
    await deploymentFinished;

    deployment = await getDeployment(deploymentID);
    expect(deployment.status).toBe('RUNNING');

    app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([nextDeploymentID]);
    expect(app.runningDeployment).toEqual(deployment);

    await runDeployment(
      firestore,
      null as unknown as Storage,
      testSecrets(),
      APP_ID,
      nextDeploymentID,
      noopScriptHandler,
    );

    const first = await getDeployment(deploymentID);
    expect(first.status).toBe('STOPPED');

    const second = await getDeployment(nextDeploymentID);
    expect(second.status).toBe('RUNNING');

    app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([]);
    expect(app.runningDeployment).toEqual(second);
  });

  test('state tracking: failure', async () => {
    const {promise: isPublishing, resolve: publishing} = resolver<void>();
    const {promise: canFinishPublishing, reject: failPublishing} =
      resolver<void>();

    // Set a running deployment on the App to make sure it is untouched.
    const runningDeployment = await writeTestDeployment('1234', 'RUNNING');
    await firestore
      .doc(appPath(APP_ID))
      .withConverter(appDataConverter)
      .update({runningDeployment});

    const deploymentID = await requestTestDeployment();
    const deploymentDoc = firestore
      .doc(deploymentPath(APP_ID, deploymentID))
      .withConverter(deploymentDataConverter);
    expect((await deploymentDoc.get()).data()?.status).toBe('REQUESTED');
    let app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([deploymentID]);
    expect(app.runningDeployment).toEqual(runningDeployment);

    const deploymentFinished = runDeployment(
      firestore,
      null as unknown as Storage,
      testSecrets(),
      APP_ID,
      deploymentID,
      {
        // eslint-disable-next-line require-yield
        async *publish() {
          publishing();
          await canFinishPublishing;
        },
        async delete() {},
      },
    );
    await isPublishing;

    expect((await deploymentDoc.get()).data()?.status).toBe('DEPLOYING');
    app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([deploymentID]);
    expect(app.runningDeployment).toEqual(runningDeployment);

    failPublishing('oh nose');
    try {
      await deploymentFinished;
    } catch (e) {
      expect(String(e)).toBe('oh nose');
    }
    const deployed = must((await deploymentDoc.get()).data());
    expect(deployed.status).toBe('FAILED');
    expect(deployed.statusMessage).toBe('There was an error deploying the app');

    app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([]);
    expect(app.runningDeployment).toEqual(runningDeployment);

    const stillRunning = await getDeployment(runningDeployment.deploymentID);
    expect(stillRunning.status).toBe('RUNNING');
  });

  test('deployment queue', async () => {
    const setTimeoutCalled = new Queue<void>();
    const setTimeoutFn = jest
      .fn()
      .mockImplementation(() => setTimeoutCalled.enqueue());

    const first = await requestTestDeployment();
    const second = await requestTestDeployment();
    const third = await requestTestDeployment();

    const doneWaiting = earlierDeployments(
      firestore,
      APP_ID,
      third,
      setTimeoutFn as unknown as typeof setTimeout,
    );

    await setTimeoutCalled.dequeue();
    expect((await getDeployment(first)).status).toBe('REQUESTED');

    // Simulate the first one finishing on its own.
    await firestore
      .doc(appPath(APP_ID))
      .withConverter(appDataConverter)
      .update({
        queuedDeploymentIDs: FieldValue.arrayRemove(first),
      });

    await setTimeoutCalled.dequeue();
    expect((await getDeployment(second)).status).toBe('REQUESTED');

    // Fire the second timeout.
    type TimeoutCallback = () => Promise<void>;
    await (setTimeoutFn.mock.calls[1][0] as unknown as TimeoutCallback)();
    expect((await getDeployment(second)).status).toBe('FAILED');

    await doneWaiting;
  });

  test('concurrent deployments', async () => {
    const id = await requestTestDeployment();
    let timesDeployed = 0;

    const testScriptHandler: ScriptHandler = {
      // eslint-disable-next-line require-await
      // eslint-disable-next-line require-yield
      async *publish() {
        timesDeployed++;
      },
      async delete() {},
    };

    const results = await Promise.allSettled([
      runDeployment(
        firestore,
        null as unknown as Storage,
        testSecrets(),
        APP_ID,
        id,
        testScriptHandler,
      ),
      runDeployment(
        firestore,
        null as unknown as Storage,
        testSecrets(),
        APP_ID,
        id,
        testScriptHandler,
      ),
    ]);

    // Verify that only one of the runs succeeded.
    expect(results[0].status).not.toBe(results[1].status);
    expect(timesDeployed).toBe(1);
  });

  test('app delete', async () => {
    // Enqueue two deployments. The delete should cancel the second (by deleting it).
    const deleteID = await requestTestDeployment('DELETE');
    const uploadID = await requestTestDeployment('USER_UPLOAD');

    // Sanity checks
    expect(
      (await firestore.doc(deploymentPath(APP_ID, deleteID)).get()).exists,
    ).toBe(true);
    expect(
      (await firestore.doc(deploymentPath(APP_ID, uploadID)).get()).exists,
    ).toBe(true);

    let scriptDeleted = false;

    await runDeployment(
      firestore,
      null as unknown as Storage,
      testSecrets(),
      APP_ID,
      deleteID,
      {
        async *publish() {},
        // eslint-disable-next-line require-await
        async delete() {
          scriptDeleted = true;
        },
      },
    );
    expect(scriptDeleted).toBe(true);

    const docs = await firestore.getAll(
      firestore.doc(appPath(APP_ID)),
      firestore.doc(envPath(APP_ID, DEFAULT_ENV)),
      firestore.doc(deploymentPath(APP_ID, deleteID)),
      firestore.doc(deploymentPath(APP_ID, uploadID)),
      firestore.doc(appNameIndexPath(TEAM_ID, APP_NAME)),
    );
    docs.forEach(doc => expect(doc.exists).toBe(false));

    const team = await getTeam(firestore, TEAM_ID);
    expect(team.numApps).toBe(0);
  });

  describe('WFP migration', () => {
    type Case = {
      name: string;
      serverVersion: string;
      scriptRef?: ScriptRef;
      expectMigration?: boolean;
    };

    const cases: Case[] = [
      {
        name: 'already WFP',
        serverVersion: SERVER_VERSION,
        scriptRef: {name: SCRIPT_NAME, namespace: NAMESPACE},
      },
      {
        name: 'unsupported version',
        serverVersion: SERVER_VERSION,
      },
      {
        name: 'supported version',
        serverVersion: WFP_SERVER_VERSION,
        expectMigration: true,
      },
      {
        name: 'supported version pre-release tag',
        serverVersion: WFP_SERVER_VERSION_PRE_RELEASE_TAG,
        expectMigration: true,
      },
    ];
    for (const c of cases) {
      test(c.name, async () => {
        if (c.scriptRef) {
          await firestore
            .doc(appPath(APP_ID))
            .withConverter(appDataConverter)
            .update({
              scriptRef: c.scriptRef,
            });
        }
        const deploymentID = await requestTestDeployment(
          'USER_UPLOAD',
          c.serverVersion,
        );

        let scriptDeleted = false;

        await runDeployment(
          firestore,
          null as unknown as Storage,
          testSecrets(),
          APP_ID,
          deploymentID,
          {
            async *publish() {},
            // eslint-disable-next-line require-await
            async delete() {
              scriptDeleted = true;
            },
          },
        );

        expect(scriptDeleted).toBe(c.expectMigration ?? false);

        const app = await getApp(firestore, APP_ID);
        if (c.expectMigration) {
          expect(app.scriptRef).toEqual({
            name: SCRIPT_NAME,
            namespace: NAMESPACE,
          });
        } else {
          expect(app.scriptRef).toEqual(c.scriptRef);
        }
      });
    }
  });
});
