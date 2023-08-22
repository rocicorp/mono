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
  appPath,
  defaultOptions,
  deploymentDataConverter,
  deploymentPath,
  deploymentsCollection,
} from 'mirror-schema/src/deployment.js';
import {setApp, dummySecrets, getApp} from 'mirror-schema/src/test-helpers.js';
import {must} from 'shared/src/must.js';
import {serverDataConverter, serverPath} from 'mirror-schema/src/server.js';
import {appDataConverter} from 'mirror-schema/src/app.js';
import {Queue} from 'shared/src/queue.js';

describe('deploy', () => {
  initializeApp({projectId: 'deploy-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'deploy-test-app-id';
  const SERVER_VERSION = 'deploy-server-version';

  beforeAll(async () => {
    await firestore
      .doc(serverPath(SERVER_VERSION))
      .withConverter(serverDataConverter)
      .create({
        major: 1,
        minor: 2,
        patch: 3,
        modules: [],
        channel: 'stable',
      });
  });

  afterAll(async () => {
    await firestore.doc(serverPath(SERVER_VERSION)).delete();
  });

  beforeEach(async () => {
    await setApp(firestore, APP_ID, {});
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

  async function requestTestDeployment(): Promise<string> {
    const deploymentPath = await requestDeployment(firestore, APP_ID, {
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
    const {promise: canFinishPublishing, resolve: finishPublishing} =
      resolver<void>();

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
      async () => {
        publishing();
        await canFinishPublishing;
      },
    );
    await isPublishing;

    deployment = await getDeployment(deploymentID);
    expect(deployment.status).toBe('DEPLOYING');
    app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([deploymentID]);
    expect(app.runningDeployment).toBeUndefined;

    // Queue up another deployment while the first is publishing.
    const nextDeploymentID = await requestTestDeployment();
    app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([deploymentID, nextDeploymentID]);
    expect(app.runningDeployment).toBeUndefined;

    finishPublishing();
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
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      async () => {},
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
      async () => {
        publishing();
        await canFinishPublishing;
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
    expect(deployed.statusMessage).toBe('oh nose');

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

    const results = await Promise.allSettled([
      runDeployment(
        firestore,
        null as unknown as Storage,
        APP_ID,
        id,
        // eslint-disable-next-line require-await
        async () => {
          timesDeployed++;
        },
      ),
      runDeployment(
        firestore,
        null as unknown as Storage,
        APP_ID,
        id,
        // eslint-disable-next-line require-await
        async () => {
          timesDeployed++;
        },
      ),
    ]);

    // Verify that only one of the runs succeeded.
    expect(results[0].status).not.toBe(results[1].status);
    expect(timesDeployed).toBe(1);
  });
});
