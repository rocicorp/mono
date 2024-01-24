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
import {FieldValue, Timestamp, getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import {
  apiKeyDataConverter,
  apiKeyPath,
  apiKeysCollection,
  defaultPermissions,
  type Permissions,
} from 'mirror-schema/src/api-key.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {setApp, setUser} from 'mirror-schema/src/test-helpers.js';
import {userPath} from 'mirror-schema/src/user.js';
import {edit, editForApp} from './edit.function.js';

// TODO: Delete after decommissioning appKeys-edit.
describe('apiKeys-edit', () => {
  // Note: The Firestore emulator returns an explanation-free UNKNOWN error if there are
  // capital letters in the projectId, so don't capitalize anything there.
  initializeApp({projectId: 'api-keys-edit-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'apiKeys-edit-test-app-id';
  const OTHER_APP_ID = 'apiKeys-edit-test-other-app-id';
  const OTHER_OTHER_APP_ID = 'apiKeys-edit-test-other-other-app-id';
  const APP_NAME = 'my-app';
  const TEAM_ID = 'my-team';
  const USER_ID = 'foo';

  beforeEach(async () => {
    await Promise.all([
      setApp(firestore, APP_ID, {
        teamID: TEAM_ID,
        name: APP_NAME,
      }),
      setUser(firestore, USER_ID, 'foo@bar.com', 'Alice', {
        [TEAM_ID]: 'admin',
      }),
      firestore
        .doc(apiKeyPath(TEAM_ID, 'existing-key'))
        .withConverter(apiKeyDataConverter)
        .create({
          value: 'foo-bar-baz',
          permissions: {'rooms:read': true} as Permissions,
          created: FieldValue.serverTimestamp(),
          lastUsed: null,
          appIDs: [APP_ID, OTHER_APP_ID],
        }),
    ]);
  });

  afterEach(async () => {
    // Clean up global emulator data.
    const batch = firestore.batch();
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(userPath(USER_ID)));
    const keys = await firestore
      .collection(apiKeysCollection(TEAM_ID))
      .listDocuments();
    keys.forEach(key => batch.delete(key));
    await batch.commit();
  });

  function callEdit(
    name: string,
    permissions: Record<string, boolean>,
    addIDs: string[] = [],
    removeIDs: string[] = [],
  ) {
    const editFunction = https.onCall(edit(firestore));
    return editFunction.run({
      data: {
        requester: {
          userID: USER_ID,
          userAgent: {type: 'reflect-cli', version: '0.36.0'},
        },
        teamID: TEAM_ID,
        name,
        permissions,
        appIDs: {
          add: addIDs,
          remove: removeIDs,
        },
      },

      auth: {
        uid: USER_ID,
        token: {email: 'foo@bar.com'} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  test('change permissions', async () => {
    const resp = await callEdit('existing-key', {'app:publish': true});
    expect(resp).toEqual({success: true});

    expect(
      (await firestore.doc(apiKeyPath(TEAM_ID, 'existing-key')).get()).data(),
    ).toMatchObject({
      value: 'foo-bar-baz',
      permissions: mergeWithDefaults({'app:publish': true}),
      created: expect.any(Timestamp),
      lastUsed: null,
      appIDs: [APP_ID, OTHER_APP_ID],
    });
  });

  test('add apps', async () => {
    const resp = await callEdit('existing-key', {'app:publish': true}, [
      OTHER_APP_ID,
      OTHER_OTHER_APP_ID,
    ]);
    expect(resp).toEqual({success: true});

    expect(
      (await firestore.doc(apiKeyPath(TEAM_ID, 'existing-key')).get()).data(),
    ).toMatchObject({
      value: 'foo-bar-baz',
      permissions: mergeWithDefaults({'app:publish': true}),
      created: expect.any(Timestamp),
      lastUsed: null,
      appIDs: [APP_ID, OTHER_APP_ID, OTHER_OTHER_APP_ID],
    });
  });

  test('remove apps', async () => {
    const resp = await callEdit(
      'existing-key',
      {'app:publish': true},
      [],
      [OTHER_APP_ID, OTHER_OTHER_APP_ID],
    );
    expect(resp).toEqual({success: true});

    expect(
      (await firestore.doc(apiKeyPath(TEAM_ID, 'existing-key')).get()).data(),
    ).toMatchObject({
      value: 'foo-bar-baz',
      permissions: mergeWithDefaults({'app:publish': true}),
      created: expect.any(Timestamp),
      lastUsed: null,
      appIDs: [APP_ID],
    });
  });

  test('add and remove apps', async () => {
    const resp = await callEdit(
      'existing-key',
      {'app:publish': true},
      [OTHER_OTHER_APP_ID],
      [APP_ID],
    );
    expect(resp).toEqual({success: true});

    expect(
      (await firestore.doc(apiKeyPath(TEAM_ID, 'existing-key')).get()).data(),
    ).toMatchObject({
      value: 'foo-bar-baz',
      permissions: mergeWithDefaults({'app:publish': true}),
      created: expect.any(Timestamp),
      lastUsed: null,
      appIDs: [OTHER_APP_ID, OTHER_OTHER_APP_ID],
    });
  });

  test('remove all apps with app:create', async () => {
    const resp = await callEdit(
      'existing-key',
      {'app:create': true},
      [],
      [APP_ID, OTHER_APP_ID],
    );
    expect(resp).toEqual({success: true});

    expect(
      (await firestore.doc(apiKeyPath(TEAM_ID, 'existing-key')).get()).data(),
    ).toMatchObject({
      value: 'foo-bar-baz',
      permissions: mergeWithDefaults({'app:create': true}),
      created: expect.any(Timestamp),
      lastUsed: null,
      appIDs: [],
    });
  });

  test('cannot add and remove same app', async () => {
    const resp = await callEdit(
      'existing-key',
      {'app:publish': true},
      [APP_ID],
      [APP_ID],
    ).catch(e => e);
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('invalid-argument');
  });

  test('cannot remove all apps', async () => {
    const resp = await callEdit(
      'existing-key',
      {'app:publish': true},
      [],
      [APP_ID, OTHER_APP_ID],
    ).catch(e => e);
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('invalid-argument');
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

// TODO: Delete after decommissioning appKeys-edit.
describe('appKeys-edit', () => {
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
        .doc(apiKeyPath(TEAM_ID, 'existing-key'))
        .withConverter(apiKeyDataConverter)
        .create({
          value: 'foo-bar-baz',
          permissions: {'rooms:read': true} as Permissions,
          created: FieldValue.serverTimestamp(),
          lastUsed: null,
          appIDs: [APP_ID],
        }),
    ]);
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

  function callEdit(name: string, permissions: Record<string, boolean>) {
    const editFunction = https.onCall(editForApp(firestore));
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
      (await firestore.doc(apiKeyPath(TEAM_ID, 'existing-key')).get()).data(),
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
