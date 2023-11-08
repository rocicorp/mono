import {afterAll, beforeAll, describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import type {Request} from 'firebase-functions/v2/https';
import {encryptUtf8} from 'mirror-schema/src/crypto.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {
  DEFAULT_ENV,
  ENCRYPTION_KEY_SECRET_NAME,
  envPath,
} from 'mirror-schema/src/env.js';
import {setApp, setEnv, setUser} from 'mirror-schema/src/test-helpers.js';
import {userPath} from 'mirror-schema/src/user.js';
import {TestSecrets} from '../../secrets/test-utils.js';
import {list} from './list.function.js';

describe('vars-list', () => {
  initializeApp({projectId: 'vars-list-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'vars-list-test-app-id';
  const APP_NAME = 'my-app';
  const TEAM_ID = 'my-team';
  const USER_ID = 'foo';

  function testSecrets() {
    return new TestSecrets(
      [ENCRYPTION_KEY_SECRET_NAME, '1', TestSecrets.TEST_KEY],
      [ENCRYPTION_KEY_SECRET_NAME, '2', TestSecrets.TEST_KEY_2],
    );
  }

  beforeAll(async () => {
    await Promise.all([
      setApp(firestore, APP_ID, {
        teamID: TEAM_ID,
        name: APP_NAME,
      }),
      setEnv(firestore, APP_ID, {
        secrets: {
          ['REFLECT_AUTH_API_KEY']: encryptUtf8(
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
            Buffer.from(TestSecrets.TEST_KEY_2, 'base64url'),
            {version: '2'},
          ),
        },
      }),
      setUser(firestore, USER_ID, 'foo@bar.com', 'Alice', {
        [TEAM_ID]: 'admin',
      }),
    ]);
  });

  afterAll(async () => {
    // Clean up global emulator data.
    const batch = firestore.batch();
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(envPath(APP_ID, DEFAULT_ENV)));
    batch.delete(firestore.doc(userPath(USER_ID)));
    await batch.commit();
  });

  function callList(decrypted: boolean) {
    const listFunction = https.onCall(list(firestore, testSecrets()));
    return listFunction.run({
      data: {
        requester: {
          userID: USER_ID,
          userAgent: {type: 'reflect-cli', version: '0.36.0'},
        },
        appID: APP_ID,
        decrypted,
      },

      auth: {
        uid: USER_ID,
        token: {email: 'foo@bar.com'} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  test('list encrypted vars', async () => {
    expect(await callList(false)).toEqual({
      success: true,
      decrypted: false,
      envs: {
        ['(default)']: {
          vars: {
            ['DB_PASSWORD']: '*****',
            ['FOO_HOSTNAME']: '*****',
          },
        },
      },
    });
  });

  test('list decrypted vars', async () => {
    expect(await callList(true)).toEqual({
      success: true,
      decrypted: true,
      envs: {
        ['(default)']: {
          vars: {
            ['DB_PASSWORD']: 'my password is so secure',
            ['FOO_HOSTNAME']: 'bar.foo.com',
          },
        },
      },
    });
  });
});
