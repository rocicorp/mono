import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import {appDataConverter} from 'mirror-schema/src/app.js';
import {encryptUtf8} from 'mirror-schema/src/crypto.js';
import {
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
  getEnv,
  setApp,
  setEnv,
  setUser,
} from 'mirror-schema/src/test-helpers.js';
import {userPath} from 'mirror-schema/src/user.js';
import {MAX_SERVER_VARIABLES} from 'mirror-schema/src/vars.js';
import {watch} from 'mirror-schema/src/watch.js';
import {SecretsCache} from '../../secrets/index.js';
import {TestSecrets} from '../../secrets/test-utils.js';
import {dummyDeployment} from '../../test-helpers.js';
import {decryptSecrets} from '../app/secrets.js';
import {set} from './set.function.js';

describe('vars-set', () => {
  initializeApp({projectId: 'vars-set-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'vars-set-test-app-id';
  const APP_NAME = 'my-app';
  const TEAM_ID = 'my-team';
  const USER_ID = 'foo';

  function testSecrets() {
    return new TestSecrets(
      [ENCRYPTION_KEY_SECRET_NAME, '1', TestSecrets.TEST_KEY],
      [ENCRYPTION_KEY_SECRET_NAME, 'latest', TestSecrets.TEST_KEY_2],
      [
        ENCRYPTION_KEY_SECRET_NAME,
        TestSecrets.LATEST_ALIAS,
        TestSecrets.TEST_KEY_2,
      ],
    );
  }

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
    batch.delete(firestore.doc(userPath(USER_ID)));
    batch.delete(firestore.doc(envPath(APP_ID, DEFAULT_ENV)));
    const deployments = await firestore
      .collection(deploymentsCollection(APP_ID))
      .listDocuments();
    for (const d of deployments) {
      batch.delete(d);
    }
    await batch.commit();
  });

  function callSet(vars: Record<string, string>) {
    const setFunction = https.onCall(set(firestore, testSecrets()));
    return setFunction.run({
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

  for (const badName of [
    'HYPHENS-NOT-ALLOWED',
    'PERIODS.NOT.ALLOWED',
    'SLASHES/NOT/ALLOWED',
    'DOLLARS$NOT$ALLOWED',
  ]) {
    test(badName, async () => {
      const result = await callSet({
        [badName]: 'new passwordz',
      }).catch(e => e);
      expect(result).toBeInstanceOf(HttpsError);
      expect((result as HttpsError).code).toBe('invalid-argument');
    });
  }

  test('variable name size limit', async () => {
    expect(
      await callSet({
        ['a'.repeat(1024)]: 'is not too big',
      }),
    ).toEqual({
      success: true,
    });

    const result = await callSet({
      ['b'.repeat(1025)]: 'is too big',
    }).catch(e => e);
    expect(result).toBeInstanceOf(HttpsError);
    expect((result as HttpsError).code).toBe('invalid-argument');
  });

  test('variable size limit', async () => {
    expect(
      await callSet({
        ['NOT_TOO_BIG']: 'a'.repeat(5100),
      }),
    ).toEqual({
      success: true,
    });

    const result = await callSet({
      ['TOO_BIG']: 'a'.repeat(5121),
    }).catch(e => e);
    expect(result).toBeInstanceOf(HttpsError);
    expect((result as HttpsError).code).toBe('invalid-argument');
  });

  test('set vars with no running deployment', async () => {
    expect(
      await callSet({
        ['DB_PASSWORD']: 'new passwordz',
        ['BAR_URL']: 'https://new-var/',
      }),
    ).toEqual({
      success: true,
    });

    const env = await getEnv(firestore, APP_ID);
    expect(env.secrets).toMatchObject({
      ['REFLECT_API_KEY']: {
        key: {version: '1'},
        iv: expect.any(Uint8Array),
        ciphertext: expect.any(Uint8Array),
      },
      ['REFLECT_VAR_FOO_HOSTNAME']: {
        key: {version: '1'},
        iv: expect.any(Uint8Array),
        ciphertext: expect.any(Uint8Array),
      },
      ['REFLECT_VAR_DB_PASSWORD']: {
        key: {version: TestSecrets.LATEST_ALIAS},
        iv: expect.any(Uint8Array),
        ciphertext: expect.any(Uint8Array),
      },
      ['REFLECT_VAR_BAR_URL']: {
        key: {version: TestSecrets.LATEST_ALIAS},
        iv: expect.any(Uint8Array),
        ciphertext: expect.any(Uint8Array),
      },
    });

    const decrypted = await decryptSecrets(
      new SecretsCache(testSecrets()),
      env.secrets,
    );
    expect(decrypted).toEqual({
      ['REFLECT_API_KEY']: 'this-is-the-reflect-auth-api-key',
      ['REFLECT_AUTH_API_KEY']: 'this-is-the-reflect-auth-api-key',
      ['REFLECT_VAR_FOO_HOSTNAME']: 'bar.foo.com',
      ['REFLECT_VAR_DB_PASSWORD']: 'new passwordz',
      ['REFLECT_VAR_BAR_URL']: 'https://new-var/',
    });
  });

  test('set vars with running deployment', async () => {
    await firestore
      .doc(appPath(APP_ID))
      .withConverter(appDataConverter)
      .update({runningDeployment: dummyDeployment('123')});

    // Kick of the function, which will wait for a deployment.
    const response = callSet({
      ['DB_PASSWORD']: 'new passwordz',
      ['NEW_VAR']: 'https://new-var/',
    });

    const envDoc = firestore
      .doc(envPath(APP_ID, DEFAULT_ENV))
      .withConverter(envDataConverter);

    // Wait for the env to be updated.
    for await (const snapshot of watch(envDoc, 10000)) {
      if (snapshot.data()?.secrets?.['REFLECT_VAR_NEW_VAR']) {
        break; // New var was written
      }
    }

    // Simulate an auto-deploy
    await firestore
      .doc(deploymentPath(APP_ID, '2345'))
      .withConverter(deploymentDataConverter)
      .create(dummyDeployment('2345'));

    expect(await response).toEqual({
      success: true,
      deploymentPath: deploymentPath(APP_ID, '2345'),
    });
  });

  test('set max vars', async () => {
    expect(
      await callSet(
        Object.fromEntries(
          // Add 48 more to the 2 variables set up in beforeEach()
          Array.from({length: MAX_SERVER_VARIABLES - 2}, (_, i) => [
            `KEY_${i}`,
            `VAL_${i}`,
          ]),
        ),
      ),
    ).toEqual({
      success: true,
    });
  });

  test('rejects more than max vars', async () => {
    const result = await callSet(
      Object.fromEntries(
        // Try to add 49 to the 2 variables set up in beforeEach()
        Array.from({length: MAX_SERVER_VARIABLES - 1}, (_, i) => [
          `KEY_${i}`,
          `VAL_${i}`,
        ]),
      ),
    ).catch(e => e);
    expect(result).toBeInstanceOf(HttpsError);
    expect((result as HttpsError).code).toBe('resource-exhausted');
  });
});
