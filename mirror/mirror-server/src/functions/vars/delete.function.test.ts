import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import type {Request} from 'firebase-functions/v2/https';
import {appDataConverter} from 'mirror-schema/src/app.js';
import {encryptUtf8} from 'mirror-schema/src/crypto.js';
import {
  appPath,
  deploymentDataConverter,
  deploymentPath,
  deploymentsCollection,
} from 'mirror-schema/src/deployment.js';
import {DEFAULT_ENV, envDataConverter, envPath} from 'mirror-schema/src/env.js';
import {
  getEnv,
  setApp,
  setEnv,
  setUser,
} from 'mirror-schema/src/test-helpers.js';
import {userPath} from 'mirror-schema/src/user.js';
import {watch} from 'mirror-schema/src/watch.js';
import {TestSecrets} from '../../secrets/test-utils.js';
import {dummyDeployment} from '../../test-helpers.js';
import {deleteFn} from './delete.function.js';

describe('vars-delete', () => {
  initializeApp({projectId: 'vars-delete-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'vars-delete-test-app-id';
  const APP_NAME = 'my-app';
  const TEAM_ID = 'my-team';
  const USER_ID = 'foo';

  beforeEach(async () => {
    await Promise.all([
      setApp(firestore, APP_ID, {
        teamID: TEAM_ID,
        name: APP_NAME,
      }),
      setEnv(firestore, APP_ID, {
        secrets: {
          ['REFLECT_API_KEY']: encryptUtf8(
            'this-is-the-reflect-auth-api-key',
            Buffer.from(TestSecrets.TEST_KEY, 'base64url'),
            {version: '1'},
          ),
          ['REFLECT_VAR_FOO_HOSTNAME']: encryptUtf8(
            'bar.foo.com',
            Buffer.from(TestSecrets.TEST_KEY, 'base64url'),
            {version: '1'},
          ),
          ['REFLECT_VAR_DB_PASSWORD']: encryptUtf8(
            'my password is so secure',
            Buffer.from(TestSecrets.TEST_KEY, 'base64url'),
            {version: '1'},
          ),
        },
      }),
      setUser(firestore, USER_ID, 'foo@bar.com', 'Alice', {
        [TEAM_ID]: 'admin',
      }),
    ]);
  });

  afterEach(async () => {
    // Clean up global emulator data.
    const batch = firestore.batch();
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(envPath(APP_ID, DEFAULT_ENV)));
    batch.delete(firestore.doc(userPath(USER_ID)));
    const deployments = await firestore
      .collection(deploymentsCollection(APP_ID))
      .listDocuments();
    for (const d of deployments) {
      batch.delete(d);
    }
    await batch.commit();
  });

  function callDelete(...vars: string[]) {
    const deleteFunction = https.onCall(deleteFn(firestore));
    return deleteFunction.run({
      data: {
        requester: {
          userID: USER_ID,
          userAgent: {type: 'reflect-cli', version: '0.36.0'},
        },
        appID: APP_ID,
        vars,
      },

      auth: {
        uid: USER_ID,
        token: {email: 'foo@bar.com'} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  test('delete vars with no running deployment', async () => {
    expect(await callDelete('DB_PASSWORD', 'NON_EXISTENT_VAR')).toEqual({
      success: true,
    });

    const env = await getEnv(firestore, APP_ID);
    expect(Object.keys(env.secrets)).toEqual([
      'REFLECT_API_KEY',
      'REFLECT_VAR_FOO_HOSTNAME',
    ]);
  });

  test('delete vars with running deployment', async () => {
    await firestore
      .doc(appPath(APP_ID))
      .withConverter(appDataConverter)
      .update({runningDeployment: dummyDeployment('123')});

    // Kick of the function, which will wait for a deployment.
    const response = callDelete('DB_PASSWORD', 'NON_EXISTENT_VAR');

    const envDoc = firestore
      .doc(envPath(APP_ID, DEFAULT_ENV))
      .withConverter(envDataConverter);

    // Wait for the env to be updated.
    for await (const snapshot of watch(envDoc, 10000)) {
      if (snapshot.data()?.secrets?.['REFLECT_VAR_DB_PASSWORD'] === undefined) {
        break; // Var has been deleted.
      }
    }

    // Simulate an auto-deploy
    await firestore
      .doc(deploymentPath(APP_ID, '9876'))
      .withConverter(deploymentDataConverter)
      .create(dummyDeployment('9876'));

    expect(await response).toEqual({
      success: true,
      deploymentPath: deploymentPath(APP_ID, '9876'),
    });
  });

  test('no-op delete with running deployment', async () => {
    const appDoc = firestore
      .doc(appPath(APP_ID))
      .withConverter(appDataConverter);

    await appDoc.update({runningDeployment: dummyDeployment('123')});
    // Kick of the function, which will wait for a deployment.
    expect(await callDelete('NON_EXISTENT_VAR1', 'NON_EXISTENT_VAR2')).toEqual({
      success: true,
    });
  });
});
