import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import {Timestamp, getFirestore} from 'firebase-admin/firestore';
import type {CloudEvent} from 'firebase-functions/v2';
import {appDataConverter} from 'mirror-schema/src/app.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {DEFAULT_ENV, envPath} from 'mirror-schema/src/env.js';
import {setApp, setEnv} from 'mirror-schema/src/test-helpers.js';
import {must} from 'shared/src/must.js';
import {autoDeploy} from './auto-deploy.function.js';

describe('env.auto-deploy', () => {
  initializeApp({projectId: 'env-auto-deploy-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'env-auto-deploy-test-app';
  const ENV_UPDATE_TIME = Timestamp.fromDate(new Date(2023, 8, 8));

  beforeEach(async () => {
    await Promise.all([
      setApp(firestore, APP_ID, {envUpdateTime: ENV_UPDATE_TIME}),
      setEnv(firestore, APP_ID, {}),
    ]);
  });

  afterEach(async () => {
    const batch = firestore.batch();
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(envPath(APP_ID, DEFAULT_ENV)));
    await batch.commit();
  });

  test('updates app.envUpdateTime', async () => {
    const envDoc = await firestore.doc(envPath(APP_ID, DEFAULT_ENV)).get();
    const envUpdateTime = must(envDoc.updateTime);

    const autoDeployFunction = autoDeploy(firestore);
    await autoDeployFunction({
      document: `apps/${APP_ID}/envs/${DEFAULT_ENV}`,
    } as unknown as CloudEvent<unknown>);

    const appDoc = await firestore
      .doc(appPath(APP_ID))
      .withConverter(appDataConverter)
      .get();
    expect(appDoc.data()?.envUpdateTime).toEqual(envUpdateTime);
  });

  test('does not move app.envUpdateTime backwards', async () => {
    const envDoc = await firestore.doc(envPath(APP_ID, DEFAULT_ENV)).get();
    const envUpdateTime = must(envDoc.updateTime);

    const laterUpdateTime = Timestamp.fromMillis(
      envUpdateTime.toMillis() + 12345,
    );

    const appDoc = firestore
      .doc(appPath(APP_ID))
      .withConverter(appDataConverter);
    await appDoc.update({envUpdateTime: laterUpdateTime});

    const autoDeployFunction = autoDeploy(firestore);
    await autoDeployFunction({
      document: `apps/${APP_ID}/envs/${DEFAULT_ENV}`,
    } as unknown as CloudEvent<unknown>);

    expect((await appDoc.get()).data()?.envUpdateTime).toEqual(laterUpdateTime);
  });
});
