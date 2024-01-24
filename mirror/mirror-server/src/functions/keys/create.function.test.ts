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
  apiKeyDataConverter,
  apiKeyPath,
  apiKeysCollection,
  defaultPermissions,
  type Permissions,
} from 'mirror-schema/src/api-key.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {setApp, setUser} from 'mirror-schema/src/test-helpers.js';
import {userPath} from 'mirror-schema/src/user.js';
import {MAX_KEYS, create, createForApp} from './create.function.js';

describe('apiKeys-create', () => {
  // Note: The Firestore emulator returns an explanation-free UNKNOWN error if there are
  // capital letters in the projectId, so don't capitalize anything there.
  initializeApp({projectId: 'app-keys-create-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'appKeys-create-test-app-id';
  const APP_NAME = 'my-app';
  const OTHER_APP_ID = 'appKeys-create-test-other-app-id';
  const OTHER_APP_NAME = 'my-other-app';
  const TEAM_ID = 'my-team';
  const USER_ID = 'foo';

  beforeAll(async () => {
    await Promise.all([
      setApp(firestore, APP_ID, {
        teamID: TEAM_ID,
        name: APP_NAME,
      }),
      setApp(firestore, OTHER_APP_ID, {
        teamID: TEAM_ID,
        name: OTHER_APP_NAME,
      }),
      setUser(firestore, USER_ID, 'foo@bar.com', 'Alice', {
        [TEAM_ID]: 'admin',
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

  function callCreate(
    name: string,
    permissions: Record<string, boolean>,
    appIDs: string[] = [APP_ID, OTHER_APP_ID],
  ) {
    const createFunction = https.onCall(create(firestore));
    return createFunction.run({
      data: {
        requester: {
          userID: USER_ID,
          userAgent: {type: 'reflect-cli', version: '0.36.0'},
        },
        teamID: TEAM_ID,
        name,
        permissions,
        appIDs,
      },

      auth: {
        uid: USER_ID,
        token: {email: 'foo@bar.com'} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  test('valid name', async () => {
    const resp = await callCreate('valid-key-name', {'app:publish': true});
    expect(resp).toMatchObject({
      success: true,
      value: expect.stringMatching(/[A-Za-z0-9_-]{40,}/),
    });

    expect(
      (await firestore.doc(apiKeyPath(TEAM_ID, 'valid-key-name')).get()).data(),
    ).toMatchObject({
      value: resp.value,
      permissions: mergeWithDefaults({'app:publish': true}),
      created: expect.any(Timestamp),
      lastUsed: null,
      appIDs: [APP_ID, OTHER_APP_ID],
    });
  });

  test('app:create with no appIDs', async () => {
    const resp = await callCreate(
      'key-with-app-create',
      {'app:create': true},
      [],
    );
    expect(resp).toMatchObject({
      success: true,
      value: expect.stringMatching(/[A-Za-z0-9_-]{40,}/),
    });

    expect(
      (
        await firestore.doc(apiKeyPath(TEAM_ID, 'key-with-app-create')).get()
      ).data(),
    ).toMatchObject({
      value: resp.value,
      permissions: mergeWithDefaults({'app:create': true}),
      created: expect.any(Timestamp),
      lastUsed: null,
      appIDs: [],
    });
  });

  for (const badName of [
    'slashes/not/allowed',
    'spaces not allowed',
    'ends-with-hyphen-',
    // Disallowed by Firestore
    '..',
    '__foo__',
  ]) {
    test(`invalid name: ${badName}`, async () => {
      const resp = await callCreate(badName, {'app:publish': true}).catch(
        e => e,
      );
      expect(resp).toBeInstanceOf(HttpsError);
      expect((resp as HttpsError).code).toBe('invalid-argument');
    });
  }

  test('no permissions', async () => {
    const resp = await callCreate('key-needs-permissions', {}).catch(e => e);
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('invalid-argument');
  });

  test('missing appIDs', async () => {
    const resp = await callCreate(
      'key-needs-appIDs',
      {'app:publish': true},
      [],
    ).catch(e => e);
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('invalid-argument');
  });

  test('invalid permissions', async () => {
    const resp = await callCreate('valid-name-but', {
      'invalid:permission': true,
    }).catch(e => e);
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('invalid-argument');
  });

  test('max keys exceeded', async () => {
    const batch = firestore.batch();
    for (let i = 0; i < MAX_KEYS; i++) {
      batch.create(
        firestore
          .doc(apiKeyPath(TEAM_ID, `key-${i}`))
          .withConverter(apiKeyDataConverter),
        {
          value: `value_${i}`,
          permissions: defaultPermissions(),
          created: FieldValue.serverTimestamp(),
          lastUsed: null,
          appIDs: [APP_ID],
        },
      );
    }
    await batch.commit();

    const resp = await callCreate('valid-name', {'app:publish': true}).catch(
      e => e,
    );
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('resource-exhausted');
  });
});

// Delete when appKeys-create is decommissioned
describe('appKeys-create', () => {
  const firestore = getFirestore();
  const APP_ID = 'appKeys-create-test-app-id';
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

  function callCreate(name: string, permissions: Record<string, boolean>) {
    const createFunction = https.onCall(createForApp(firestore));
    return createFunction.run({
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

  test('valid name', async () => {
    const resp = await callCreate('valid-key-name', {'app:publish': true});
    expect(resp).toMatchObject({
      success: true,
      value: expect.stringMatching(/[A-Za-z0-9_-]{40,}/),
    });

    expect(
      (await firestore.doc(apiKeyPath(TEAM_ID, 'valid-key-name')).get()).data(),
    ).toMatchObject({
      value: resp.value,
      permissions: mergeWithDefaults({'app:publish': true}),
      created: expect.any(Timestamp),
      lastUsed: null,
    });
  });

  for (const badName of [
    'slashes/not/allowed',
    'spaces not allowed',
    'ends-with-hyphen-',
    // Disallowed by Firestore
    '..',
    '__foo__',
  ]) {
    test(`invalid name: ${badName}`, async () => {
      const resp = await callCreate(badName, {'app:publish': true}).catch(
        e => e,
      );
      expect(resp).toBeInstanceOf(HttpsError);
      expect((resp as HttpsError).code).toBe('invalid-argument');
    });
  }

  test('no permissions', async () => {
    const resp = await callCreate('key-needs-permissions', {}).catch(e => e);
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('invalid-argument');
  });

  test('invalid permissions', async () => {
    const resp = await callCreate('valid-name-but', {
      'invalid:permission': true,
    }).catch(e => e);
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('invalid-argument');
  });

  test('max keys exceeded', async () => {
    const batch = firestore.batch();
    for (let i = 0; i < MAX_KEYS; i++) {
      batch.create(
        firestore
          .doc(apiKeyPath(TEAM_ID, `key-${i}`))
          .withConverter(apiKeyDataConverter),
        {
          value: `value_${i}`,
          permissions: defaultPermissions(),
          created: FieldValue.serverTimestamp(),
          lastUsed: null,
          appIDs: [APP_ID],
        },
      );
    }
    await batch.commit();

    const resp = await callCreate('valid-name', {'app:publish': true}).catch(
      e => e,
    );
    expect(resp).toBeInstanceOf(HttpsError);
    expect((resp as HttpsError).code).toBe('resource-exhausted');
  });
});

function mergeWithDefaults(perms: Partial<Permissions>): Permissions {
  return {
    ...defaultPermissions(),
    ...perms,
  };
}
