import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {FieldValue, getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import type {Request} from 'firebase-functions/v2/https';
import {
  apiKeyDataConverter,
  apiKeyPath,
  apiKeysCollection,
  type Permissions,
} from 'mirror-schema/src/api-key.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {setApp, setUser} from 'mirror-schema/src/test-helpers.js';
import {userPath} from 'mirror-schema/src/user.js';
import {deleteFn, deleteForApp} from './delete.function.js';

describe('apiKeys-delete', () => {
  // Note: The Firestore emulator returns an explanation-free UNKNOWN error if there are
  // capital letters in the projectId, so don't capitalize anything there.
  initializeApp({projectId: 'api-keys-delete-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'appKeys-delete-test-app-id';
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
    ]);
  });

  beforeEach(async () => {
    const batch = firestore.batch();
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].forEach(i => {
      batch.create(
        firestore
          .doc(apiKeyPath(TEAM_ID, `key-${i}`))
          .withConverter(apiKeyDataConverter),
        {
          value: `foo-bar-baz-${i}`,
          permissions: {'rooms:read': true} as Permissions,
          created: FieldValue.serverTimestamp(),
          lastUsed: null,
          appIDs: [APP_ID],
        },
      );
    });
    await batch.commit();
  });

  afterEach(async () => {
    const batch = firestore.batch();
    const keys = await firestore
      .collection(apiKeysCollection(TEAM_ID))
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

  function callDelete(...names: string[]) {
    const deleteFunction = https.onCall(deleteFn(firestore));
    return deleteFunction.run({
      data: {
        requester: {
          userID: USER_ID,
          userAgent: {type: 'reflect-cli', version: '0.36.0'},
        },
        teamID: TEAM_ID,
        names,
      },

      auth: {
        uid: USER_ID,
        token: {email: 'foo@bar.com'} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  test('delete', async () => {
    const resp = await callDelete(
      'key-8',
      'key-bar',
      'key-2',
      'key-5',
      'key-foo',
    );
    expect(resp).toEqual({
      success: true,
      deleted: ['key-8', 'key-2', 'key-5'],
    });

    expect(
      (
        await firestore.collection(apiKeysCollection(TEAM_ID)).listDocuments()
      ).map(doc => doc.id),
    ).toEqual(['key-0', 'key-1', 'key-3', 'key-4', 'key-6', 'key-7', 'key-9']);
  });
});

// TODO: Delete after decommissioning appKeys-delete
describe('appKeys-delete', () => {
  const firestore = getFirestore();
  const APP_ID = 'appKeys-delete-test-app-id';
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
    ]);
  });

  beforeEach(async () => {
    const batch = firestore.batch();
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].forEach(i => {
      batch.create(
        firestore
          .doc(apiKeyPath(TEAM_ID, `key-${i}`))
          .withConverter(apiKeyDataConverter),
        {
          value: `foo-bar-baz-${i}`,
          permissions: {'rooms:read': true} as Permissions,
          created: FieldValue.serverTimestamp(),
          lastUsed: null,
          appIDs: [APP_ID],
        },
      );
    });
    await batch.commit();
  });

  afterEach(async () => {
    const batch = firestore.batch();
    const keys = await firestore
      .collection(apiKeysCollection(TEAM_ID))
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

  function callDelete(...names: string[]) {
    const deleteFunction = https.onCall(deleteForApp(firestore));
    return deleteFunction.run({
      data: {
        requester: {
          userID: USER_ID,
          userAgent: {type: 'reflect-cli', version: '0.36.0'},
        },
        appID: APP_ID,
        names,
      },

      auth: {
        uid: USER_ID,
        token: {email: 'foo@bar.com'} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  test('delete', async () => {
    const resp = await callDelete(
      'key-8',
      'key-bar',
      'key-2',
      'key-5',
      'key-foo',
    );
    expect(resp).toEqual({
      success: true,
      deleted: ['key-8', 'key-2', 'key-5'],
    });

    expect(
      (
        await firestore.collection(apiKeysCollection(TEAM_ID)).listDocuments()
      ).map(doc => doc.id),
    ).toEqual(['key-0', 'key-1', 'key-3', 'key-4', 'key-6', 'key-7', 'key-9']);
  });
});
