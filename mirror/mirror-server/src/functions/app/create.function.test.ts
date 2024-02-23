import {afterEach, beforeEach, describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {Timestamp, getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import {
  Permissions,
  apiKeyDataConverter,
  apiKeyPath,
} from 'mirror-schema/src/api-key.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {ENCRYPTION_KEY_SECRET_NAME} from 'mirror-schema/src/env.js';
import {teamMembershipPath} from 'mirror-schema/src/membership.js';
import {
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';
import type {StandardReleaseChannel} from 'mirror-schema/src/server.js';
import {
  appNameIndexPath,
  teamDataConverter,
  teamPath,
} from 'mirror-schema/src/team.js';
import {
  getApp,
  getAppName,
  getEnv,
  getTeam,
  setTeam,
  setUser,
} from 'mirror-schema/src/test-helpers.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {SemVer} from 'semver';
import {TestSecrets} from '../../secrets/test-utils.js';
import type {DistTags} from '../validators/version.js';
import {MIN_WFP_VERSION, create} from './create.function.js';

describe('app-create function', () => {
  initializeApp({projectId: 'app-create-function-test'});
  const firestore = getFirestore();
  const USER_ID = 'app-create-test-user';
  const USER_EMAIL = 'foo@bar.com';
  const PROVIDER = 'tuesday';
  const CF_ID = 'cf-123';
  const TEAM_ID = 'app-create-test-team';
  const TEAM_LABEL = 'footeam';
  const API_KEY_NAME = 'app-creator';

  function callCreate(
    appName: string,
    userID = USER_ID,
    reflectVersion = '0.35.0',
    serverReleaseChannel?: string,
    testDistTags: DistTags = {},
  ) {
    const createFunction = https.onCall(
      create(
        firestore,
        new TestSecrets([
          ENCRYPTION_KEY_SECRET_NAME,
          'latest',
          TestSecrets.TEST_KEY,
        ]),
        testDistTags,
      ),
    );

    return createFunction.run({
      data: {
        requester: {
          userID,
          userAgent: {type: 'reflect-cli', version: reflectVersion},
        },
        teamID: TEAM_ID,
        name: appName,
        serverReleaseChannel: (serverReleaseChannel ??
          'stable') as StandardReleaseChannel,
      },

      auth: {
        uid: userID,
        token: {email: USER_EMAIL} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  async function deleteApp(appID: string, appName: string): Promise<void> {
    const batch = firestore.batch();
    batch.delete(firestore.doc(appPath(appID)));
    batch.delete(firestore.doc(appNameIndexPath(TEAM_ID, appName)));
    await batch.commit();
  }

  beforeEach(async () => {
    await Promise.all([
      setUser(firestore, USER_ID, USER_EMAIL, 'Alice', {
        [TEAM_ID]: 'admin',
      }),
      firestore
        .doc(providerPath(PROVIDER))
        .withConverter(providerDataConverter)
        .create({
          accountID: CF_ID,
          defaultMaxApps: 3,
          defaultZone: {
            zoneID: 'zone-id',
            zoneName: 'reflect-o-rama.net',
          },
          dispatchNamespace: 'prod',
        }),
      setTeam(firestore, TEAM_ID, {
        defaultCfID: 'deprecated',
        defaultProvider: PROVIDER,
        label: TEAM_LABEL,
        numAdmins: 1,
        numApps: 2,
        maxApps: 5,
      }),
      firestore
        .doc(apiKeyPath(TEAM_ID, API_KEY_NAME))
        .withConverter(apiKeyDataConverter)
        .create({
          value: 'secret',
          permissions: {'app:create': true} as Permissions,
          lastUsed: null,
          created: Timestamp.fromMillis(Date.UTC(2024, 0, 12)),
          appIDs: ['other-app-id'],
        }),
    ]);
  });

  // Clean up test data from global emulator state.
  afterEach(async () => {
    const batch = firestore.batch();
    for (const doc of [
      firestore.doc(userPath(USER_ID)),
      firestore.doc(teamPath(TEAM_ID)),
      firestore.doc(teamMembershipPath(TEAM_ID, USER_ID)),
      firestore.doc(providerPath(PROVIDER)),
      firestore.doc(apiKeyPath(TEAM_ID, API_KEY_NAME)),
    ]) {
      batch.delete(doc);
    }
    await batch.commit();
  });

  test('create app as admin', async () => {
    const appName = 'my-app';
    const resp = await callCreate(appName);
    expect(resp).toMatchObject({
      success: true,
      appID: expect.any(String),
    });

    const app = await getApp(firestore, resp.appID);
    expect(app).toMatchObject({
      teamID: TEAM_ID,
      teamLabel: TEAM_LABEL,
      name: appName,
      provider: PROVIDER,
      cfScriptName: expect.any(String),
      serverReleaseChannel: 'stable',
      envUpdateTime: expect.any(Timestamp),
    });
    // Not a WFP app.
    expect(app.scriptRef).toBeUndefined;

    const env = await getEnv(firestore, resp.appID);
    expect(env).toMatchObject({
      deploymentOptions: {
        vars: {
          /* eslint-disable @typescript-eslint/naming-convention */
          DISABLE_LOG_FILTERING: 'false',
          LOG_LEVEL: 'info',
          /* eslint-enable @typescript-eslint/naming-convention */
        },
      },
      secrets: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        REFLECT_API_KEY: {
          key: {version: TestSecrets.LATEST_ALIAS},
          iv: expect.any(Uint8Array),
          ciphertext: expect.any(Uint8Array),
        },
      },
    });

    const team = await getTeam(firestore, TEAM_ID);
    expect(team.numApps).toBe(3); // This was initialized with 2 in beforeEach()

    const appNameEntry = await getAppName(firestore, TEAM_ID, appName);
    expect(appNameEntry).toEqual({
      appID: resp.appID,
    });

    // Cleanup
    await deleteApp(resp.appID, appName);
  });

  test('create app with api key', async () => {
    const appName = 'my-app';
    const keyPath = `teams/${TEAM_ID}/keys/${API_KEY_NAME}`;
    const resp = await callCreate(appName, keyPath);
    expect(resp).toMatchObject({
      success: true,
      appID: expect.any(String),
    });

    const app = await getApp(firestore, resp.appID);
    expect(app).toMatchObject({
      teamID: TEAM_ID,
      teamLabel: TEAM_LABEL,
      name: appName,
      provider: PROVIDER,
      cfScriptName: expect.any(String),
      serverReleaseChannel: 'stable',
      envUpdateTime: expect.any(Timestamp),
    });
    // Not a WFP app.
    expect(app.scriptRef).toBeUndefined;

    const env = await getEnv(firestore, resp.appID);
    expect(env).toMatchObject({
      deploymentOptions: {
        vars: {
          /* eslint-disable @typescript-eslint/naming-convention */
          DISABLE_LOG_FILTERING: 'false',
          LOG_LEVEL: 'info',
          /* eslint-enable @typescript-eslint/naming-convention */
        },
      },
      secrets: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        REFLECT_API_KEY: {
          key: {version: TestSecrets.LATEST_ALIAS},
          iv: expect.any(Uint8Array),
          ciphertext: expect.any(Uint8Array),
        },
      },
    });

    const team = await getTeam(firestore, TEAM_ID);
    expect(team.numApps).toBe(3); // This was initialized with 2 in beforeEach()

    const appNameEntry = await getAppName(firestore, TEAM_ID, appName);
    expect(appNameEntry).toEqual({
      appID: resp.appID,
    });

    const apiKey = await firestore.doc(keyPath).get();
    expect(apiKey.data()).toEqual({
      value: 'secret',
      permissions: {'app:create': true},
      created: Timestamp.fromMillis(Date.UTC(2024, 0, 12)),
      lastUsed: null,
      appIDs: ['other-app-id', resp.appID], // new appID is added
    });

    // Cleanup
    await deleteApp(resp.appID, appName);
  });

  describe('create WFP app', () => {
    const minWFPRelease = MIN_WFP_VERSION.raw;
    for (const release of [minWFPRelease, `${minWFPRelease}-canary.0`]) {
      test(`release ${release}`, async () => {
        const appName = 'my-app';
        const resp = await callCreate(appName, USER_ID, release);
        expect(resp).toMatchObject({
          success: true,
          appID: expect.any(String),
        });

        const app = await getApp(firestore, resp.appID);
        expect(app).toMatchObject({
          teamID: TEAM_ID,
          teamLabel: TEAM_LABEL,
          name: appName,
          provider: PROVIDER,
          cfScriptName: expect.any(String),
          scriptRef: {
            namespace: 'prod',
            name: app.cfScriptName,
          },
          serverReleaseChannel: 'stable',
          envUpdateTime: expect.any(Timestamp),
        });

        const env = await getEnv(firestore, resp.appID);
        expect(env).toMatchObject({
          deploymentOptions: {
            vars: {
              /* eslint-disable @typescript-eslint/naming-convention */
              DISABLE_LOG_FILTERING: 'false',
              LOG_LEVEL: 'info',
              /* eslint-enable @typescript-eslint/naming-convention */
            },
          },
          secrets: {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            REFLECT_API_KEY: {
              key: {version: TestSecrets.LATEST_ALIAS},
              iv: expect.any(Uint8Array),
              ciphertext: expect.any(Uint8Array),
            },
          },
        });
        const team = await getTeam(firestore, TEAM_ID);
        expect(team.numApps).toBe(3); // This was initialized with 2 in beforeEach()

        const appNameEntry = await getAppName(firestore, TEAM_ID, appName);
        expect(appNameEntry).toEqual({
          appID: resp.appID,
        });

        // Cleanup
        await deleteApp(resp.appID, appName);
      });
    }
  });

  test('cannot create app on non-standard release channel', async () => {
    const appName = 'my-app';
    try {
      await callCreate(appName, USER_ID, '0.35.0', 'debug');
      throw new Error('Expected invalid-argument');
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('invalid-argument');
    }

    const resp = await callCreate(appName, USER_ID, '0.35.0', 'canary');
    expect(resp).toMatchObject({
      success: true,
      appID: expect.any(String),
    });

    await deleteApp(resp.appID, appName);
  });

  test('cannot create app with deprecated cli', async () => {
    const appName = 'my-app';
    try {
      await callCreate(appName, USER_ID, '0.35.0', undefined, {
        rec: new SemVer('0.35.1'),
      });
      throw new Error('Expected out-of-range');
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('out-of-range');
    }

    const resp = await callCreate(appName, USER_ID, '0.35.1', undefined, {
      rec: new SemVer('0.35.1'),
    });
    expect(resp).toMatchObject({
      success: true,
      appID: expect.any(String),
    });

    await deleteApp(resp.appID, appName);
  });

  test('cannot create app as non-admin', async () => {
    await firestore
      .doc(userPath(USER_ID))
      .withConverter(userDataConverter)
      .update({roles: {[TEAM_ID]: 'member'}});

    const appName = 'my-app';
    try {
      await callCreate(appName);
      throw new Error('Expected permission-denied');
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('permission-denied');
    }
  });

  test('cannot create app when explicit app limit exceeded', async () => {
    await firestore
      .doc(teamPath(TEAM_ID))
      .withConverter(teamDataConverter)
      .update({numApps: 4, maxApps: 4});

    const appName = 'my-app';
    try {
      await callCreate(appName);
      throw new Error('Expected resource-exhausted');
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('resource-exhausted');
    }
  });

  test('cannot create app when default app limit exceeded', async () => {
    await firestore
      .doc(teamPath(TEAM_ID))
      .withConverter(teamDataConverter)
      .update({numApps: 3, maxApps: null}); // Cloudflare.defaultMaxApps set to 3.

    const appName = 'my-app';
    try {
      await callCreate(appName);
      throw new Error('Expected resource-exhausted');
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('resource-exhausted');
    }
  });

  for (const appName of [
    '0starts-with-a-number',
    'ends-with-a-hyphen-',
    'has.invalid.characters',
  ]) {
    test(`invalid app name: ${appName}`, async () => {
      try {
        await callCreate(appName);
        throw new Error('Expected invalid-argument');
      } catch (e) {
        console.error(e);
        expect(e).toBeInstanceOf(HttpsError);
        expect((e as HttpsError).code).toBe('invalid-argument');
      }
    });
  }
});
