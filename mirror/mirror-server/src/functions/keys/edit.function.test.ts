import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  test,
} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {FieldValue, Timestamp, getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import {
  appKeyDataConverter,
  appKeyPath,
  appKeysCollection,
  defaultPermissions,
  type Permissions,
} from 'mirror-schema/src/api-key.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {setApp, setUser} from 'mirror-schema/src/test-helpers.js';
import {userPath} from 'mirror-schema/src/user.js';
import {edit} from './edit.function.js';

describe('appKeys-edit', () => {
  // Note: The Firestore emulator returns an explanation-free UNKNOWN error if there are
  // capital letters in the projectId, so don't capitalize anything there.
  initializeApp({projectId: 'app-keys-edit-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'appKeys-edit-test-app-id';
  const APP_NAME = 'my-app';
  const TEAM_ID = 'my-team';
  const USER_ID = 'foo';

  beforeAll(async () => {
    await Promise.all([
      setApp(firestore, APP_ID, {
        teamID: TEAM_ID,
        name: APP_NAME,
      }),
      setUser(firestore, USER_ID, 'foo@bar.com', 'Alice', {
        [TEAM_ID]: 'admin',
      }),
      firestore
        .doc(appKeyPath(APP_ID, 'existing-key'))
        .withConverter(appKeyDataConverter)
        .create({
          value: 'foo-bar-baz',
          permissions: {'rooms:read': true} as Permissions,
          created: FieldValue.serverTimestamp(),
          lastUsed: null,
        }),
    ]);
  });

  afterEach(async () => {
    const batch = firestore.batch();
    const keys = await firestore
      .collection(appKeysCollection(APP_ID))
      .listDocuments();
    keys.forEach(key => batch.delete(key));
    await batch.commit();
  });

  afterAll(async () => {
    // Clean up global emulator data.
    const batch = firestore.batch();
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(userPath(USER_ID)));
    await batch.commit();
  });

  function callEdit(name: string, permissions: Record<string, boolean>) {
    const editFunction = https.onCall(edit(firestore));
    return editFunction.run({
      data: {
        requester: {
          userID: USER_ID,
          userAgent: {type: 'reflect-cli', version: '0.36.0'},
        },
        appID: APP_ID,
        name,
        permissions,
      },

      auth: {
        uid: USER_ID,
        token: {email: 'foo@bar.com'} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  test('existing key', async () => {
    const resp = await callEdit('existing-key', {'app:publish': true});
    expect(resp).toEqual({success: true});

    expect(
      (await firestore.doc(appKeyPath(APP_ID, 'existing-key')).get()).data(),
    ).toMatchObject({
      value: 'foo-bar-baz',
      permissions: mergeWithDefaults({'app:publish': true}),
      created: expect.any(Timestamp),
      lastUsed: null,
    });
  });

  test('invalid permissions', async () => {
    const resp = await callEdit('existing-key', {
      'invalid:permissions': true,
    }).catch(e => e);
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('invalid-argument');
  });

  test('invalid permissions', async () => {
    const resp = await callEdit('existing-key', {}).catch(e => e);
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('invalid-argument');
  });

  test('non-existent key', async () => {
    const resp = await callEdit('non-existing-key', {
      'app:publish': true,
    }).catch(e => e);
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('not-found');
  });
});

function mergeWithDefaults(perms: Partial<Permissions>): Permissions {
  return {
    ...defaultPermissions(),
    ...perms,
  };
}
