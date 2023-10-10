import {
  describe,
  test,
  jest,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
  afterEach,
} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import {FieldValue, Timestamp, getFirestore} from 'firebase-admin/firestore';
import {resolver} from '@rocicorp/resolver';
import {
  runDeployment,
  earlierDeployments,
  requestDeployment,
} from './deploy.function.js';
import type {Storage} from 'firebase-admin/storage';
import {
  Deployment,
  DeploymentStatus,
  DeploymentType,
  appPath,
  defaultOptions,
  deploymentDataConverter,
  deploymentPath,
  deploymentsCollection,
} from 'mirror-schema/src/deployment.js';
import {
  setApp,
  dummySecrets,
  getApp,
  setTeam,
  setAppName,
  getTeam,
} from 'mirror-schema/src/test-helpers.js';
import {must} from 'shared/src/must.js';
import {serverDataConverter, serverPath} from 'mirror-schema/src/server.js';
import {type ScriptRef, appDataConverter} from 'mirror-schema/src/app.js';
import {Queue} from 'shared/src/queue.js';
import {mockFunctionParamsAndSecrets} from '../../test-helpers.js';
import {appNameIndexPath, teamPath} from 'mirror-schema/src/team.js';
import {sleep} from 'shared/src/sleep.js';
import {
  DEFAULT_PROVIDER_ID,
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';
import type {ScriptHandler} from '../../cloudflare/script-handler.js';
import {MIN_WFP_VERSION} from './create.function.js';

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

  const noopScriptHandler: ScriptHandler = {
    async *publish(): AsyncGenerator<string> {},
    async delete(): Promise<void> {},
  };

  function mockGetApiToken(provider: string): Promise<string> {
    expect(provider).toBe(DEFAULT_PROVIDER_ID);
    return Promise.resolve('api-token');
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
    await setApp(firestore, APP_ID, {
      teamID: TEAM_ID,
      name: APP_NAME,
      cfScriptName: SCRIPT_NAME,
    });
    await setTeam(firestore, TEAM_ID, {numApps: 1});
    await setAppName(firestore, TEAM_ID, APP_ID, APP_NAME);
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
        options: defaultOptions(),
        hashesOfSecrets: dummySecrets(),
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
        options: defaultOptions(),
        hashesOfSecrets: dummySecrets(),
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
      APP_ID,
      deploymentID,
      mockGetApiToken,
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
      APP_ID,
      nextDeploymentID,
      mockGetApiToken,
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
      APP_ID,
      deploymentID,
      mockGetApiToken,
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
        APP_ID,
        id,
        mockGetApiToken,
        testScriptHandler,
      ),
      runDeployment(
        firestore,
        null as unknown as Storage,
        APP_ID,
        id,
        mockGetApiToken,
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
      APP_ID,
      deleteID,
      mockGetApiToken,
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
          APP_ID,
          deploymentID,
          mockGetApiToken,
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
