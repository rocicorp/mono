import {afterAll, beforeAll, describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {Timestamp, getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import type {Request} from 'firebase-functions/v2/https';
import {
  ALL_PERMISSIONS,
  APP_CREATE_PERMISSION,
  apiKeyDataConverter,
  apiKeyPath,
  apiKeysCollection,
  defaultPermissions,
  type Permissions,
} from 'mirror-schema/src/api-key.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {setApp, setUser} from 'mirror-schema/src/test-helpers.js';
import {userPath} from 'mirror-schema/src/user.js';
import {list, listForApp} from './list.function.js';

describe('apiKeys-list', () => {
  // Note: The Firestore emulator returns an explanation-free UNKNOWN error if there are
  // capital letters in the projectId, so don't capitalize anything there.
  initializeApp({projectId: 'api-keys-list-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'apiKeys-list-test-app-id';
  const OTHER_APP_ID = 'apiKeys-list-test-other-app-id';
  const APP_NAME = 'my-app';
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

  afterAll(async () => {
    // Clean up global emulator data.
    const batch = firestore.batch();
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(appPath(OTHER_APP_ID)));
    batch.delete(firestore.doc(userPath(USER_ID)));
    await batch.commit();
  });

  function callList(show: boolean) {
    const listFunction = https.onCall(list(firestore));
    return listFunction.run({
      data: {
        requester: {
          userID: USER_ID,
          userAgent: {type: 'reflect-cli', version: '0.36.0'},
        },
        teamID: TEAM_ID,
        show,
      },

      auth: {
        uid: USER_ID,
        token: {email: 'foo@bar.com'} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  test('no keys', async () => {
    expect(await callList(false)).toEqual({
      success: true,
      keys: [],
      allPermissions: ALL_PERMISSIONS,
    });
  });

  describe('with keys', () => {
    beforeAll(async () => {
      await Promise.all([
        firestore
          .doc(apiKeyPath(TEAM_ID, 'my-publish-key'))
          .withConverter(apiKeyDataConverter)
          .create({
            value: '12345678',
            permissions: {'app:publish': true} as Permissions,
            created: Timestamp.fromMillis(Date.UTC(2023, 11, 0)),
            lastUsed: null,
            appIDs: [APP_ID],
          }),
        firestore
          .doc(apiKeyPath(TEAM_ID, 'my-reflect-api-key'))
          .withConverter(apiKeyDataConverter)
          .create({
            value: 'abcdefg',
            permissions: {'rooms:read': true} as Permissions,
            created: Timestamp.fromMillis(Date.UTC(2023, 10, 0)),
            lastUsed: Timestamp.fromMillis(Date.UTC(2023, 11, 1)),
            appIDs: [APP_ID, OTHER_APP_ID],
          }),
        firestore
          .doc(apiKeyPath(TEAM_ID, 'my-other-api-key'))
          .withConverter(apiKeyDataConverter)
          .create({
            value: '!@#$%^',
            permissions: {'connections:invalidate': true} as Permissions,
            created: Timestamp.fromMillis(Date.UTC(2023, 10, 0)),
            lastUsed: Timestamp.fromMillis(Date.UTC(2023, 10, 2)),
            appIDs: [OTHER_APP_ID],
          }),
      ]);
    });

    afterAll(async () => {
      // Clean up global emulator data.
      const batch = firestore.batch();
      const keys = await firestore
        .collection(apiKeysCollection(TEAM_ID))
        .listDocuments();
      keys.forEach(key => batch.delete(key));
      await batch.commit();
    });

    test('list hidden keys', async () => {
      expect(await callList(false)).toEqual({
        success: true,
        keys: [
          {
            name: 'my-reflect-api-key',
            value: null,
            permissions: mergeWithDefaults({'rooms:read': true}),
            createTime: 1698710400000,
            lastUseTime: 1701388800000,
            apps: {
              [APP_ID]: APP_NAME,
              [OTHER_APP_ID]: OTHER_APP_NAME,
            },
          },
          {
            name: 'my-other-api-key',
            value: null,
            permissions: mergeWithDefaults({'connections:invalidate': true}),
            createTime: 1698710400000,
            lastUseTime: 1698883200000,
            apps: {[OTHER_APP_ID]: OTHER_APP_NAME},
          },
          {
            name: 'my-publish-key',
            value: null,
            permissions: mergeWithDefaults({'app:publish': true}),
            createTime: 1701302400000,
            lastUseTime: null,
            apps: {[APP_ID]: APP_NAME},
          },
        ],
        allPermissions: ALL_PERMISSIONS,
      });
    });

    test('list unhidden keys', async () => {
      expect(await callList(true)).toEqual({
        success: true,
        keys: [
          {
            name: 'my-reflect-api-key',
            value: 'abcdefg',
            permissions: mergeWithDefaults({'rooms:read': true}),
            createTime: 1698710400000,
            lastUseTime: 1701388800000,
            apps: {
              [APP_ID]: APP_NAME,
              [OTHER_APP_ID]: OTHER_APP_NAME,
            },
          },
          {
            name: 'my-other-api-key',
            value: '!@#$%^',
            permissions: mergeWithDefaults({'connections:invalidate': true}),
            createTime: 1698710400000,
            lastUseTime: 1698883200000,
            apps: {[OTHER_APP_ID]: OTHER_APP_NAME},
          },
          {
            name: 'my-publish-key',
            value: '12345678',
            permissions: mergeWithDefaults({'app:publish': true}),
            createTime: 1701302400000,
            lastUseTime: null,
            apps: {[APP_ID]: APP_NAME},
          },
        ],
        allPermissions: ALL_PERMISSIONS,
      });
    });
  });
});

// TODO: Delete when appKeys-list is decommissioned
describe('appKeys-list', () => {
  function allLegacyPermissions(): Record<string, string> {
    const legacyPermissions: Record<string, string> = {...ALL_PERMISSIONS};
    delete legacyPermissions[APP_CREATE_PERMISSION];
    return legacyPermissions;
  }

  const firestore = getFirestore();
  const APP_ID = 'appKeys-list-test-app-id';
  const OTHER_APP_ID = 'appKeys-list-test-other-app-id';
  const APP_NAME = 'my-app';
  const TEAM_ID = 'my-team';
  const USER_ID = 'foo';
  const LEGACY_PERMISSIONS = allLegacyPermissions();

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

  afterAll(async () => {
    // Clean up global emulator data.
    const batch = firestore.batch();
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(userPath(USER_ID)));
    await batch.commit();
  });

  function callList(show: boolean) {
    const listFunction = https.onCall(listForApp(firestore));
    return listFunction.run({
      data: {
        requester: {
          userID: USER_ID,
          userAgent: {type: 'reflect-cli', version: '0.36.0'},
        },
        appID: APP_ID,
        show,
      },

      auth: {
        uid: USER_ID,
        token: {email: 'foo@bar.com'} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  test('no keys', async () => {
    expect(await callList(false)).toEqual({
      success: true,
      keys: [],
      allPermissions: LEGACY_PERMISSIONS,
    });
  });

  describe('with keys', () => {
    beforeAll(async () => {
      await Promise.all([
        firestore
          .doc(apiKeyPath(TEAM_ID, 'my-publish-key'))
          .withConverter(apiKeyDataConverter)
          .create({
            value: '12345678',
            permissions: {'app:publish': true} as Permissions,
            created: Timestamp.fromMillis(Date.UTC(2023, 11, 0)),
            lastUsed: null,
            appIDs: [APP_ID],
          }),
        firestore
          .doc(apiKeyPath(TEAM_ID, 'my-reflect-api-key'))
          .withConverter(apiKeyDataConverter)
          .create({
            value: 'abcdefg',
            permissions: {'rooms:read': true} as Permissions,
            created: Timestamp.fromMillis(Date.UTC(2023, 10, 0)),
            lastUsed: Timestamp.fromMillis(Date.UTC(2023, 11, 1)),
            appIDs: [APP_ID, OTHER_APP_ID],
          }),
        firestore
          .doc(apiKeyPath(TEAM_ID, 'my-other-api-key'))
          .withConverter(apiKeyDataConverter)
          .create({
            value: '!@#$%^',
            permissions: {'rooms:read': true} as Permissions,
            created: Timestamp.fromMillis(Date.UTC(2023, 10, 0)),
            lastUsed: Timestamp.fromMillis(Date.UTC(2023, 11, 1)),
            appIDs: [OTHER_APP_ID],
          }),
      ]);
    });

    afterAll(async () => {
      // Clean up global emulator data.
      const batch = firestore.batch();
      const keys = await firestore
        .collection(apiKeysCollection(TEAM_ID))
        .listDocuments();
      keys.forEach(key => batch.delete(key));
      await batch.commit();
    });

    test('list hidden keys', async () => {
      expect(await callList(false)).toEqual({
        success: true,
        keys: [
          {
            name: 'my-reflect-api-key',
            value: null,
            permissions: mergeWithDefaults({'rooms:read': true}),
            createTime: 1698710400000,
            lastUseTime: 1701388800000,
          },
          {
            name: 'my-publish-key',
            value: null,
            permissions: mergeWithDefaults({'app:publish': true}),
            createTime: 1701302400000,
            lastUseTime: null,
          },
        ],
        allPermissions: LEGACY_PERMISSIONS,
      });
    });

    test('list unhidden keys', async () => {
      expect(await callList(true)).toEqual({
        success: true,
        keys: [
          {
            name: 'my-reflect-api-key',
            value: 'abcdefg',
            permissions: mergeWithDefaults({'rooms:read': true}),
            createTime: 1698710400000,
            lastUseTime: 1701388800000,
          },
          {
            name: 'my-publish-key',
            value: '12345678',
            permissions: mergeWithDefaults({'app:publish': true}),
            createTime: 1701302400000,
            lastUseTime: null,
          },
        ],
        allPermissions: LEGACY_PERMISSIONS,
      });
    });
  });
});

function mergeWithDefaults(perms: Partial<Permissions>): Permissions {
  return {
    ...defaultPermissions(),
    ...perms,
  };
}
