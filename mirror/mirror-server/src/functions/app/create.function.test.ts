import {describe, expect, test, afterEach} from '@jest/globals';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {getFirestore, type Firestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import {
  type Membership,
  teamMembershipPath,
} from 'mirror-schema/src/membership.js';
import {
  getApp,
  getAppName,
  getMembership,
  getTeam,
  getUser,
  setMembership,
  setTeam,
  setUser,
} from 'mirror-schema/src/test-helpers.js';
import {mockFunctionParamsAndSecrets} from '../../test-helpers.js';
import {DEFAULT_MAX_APPS, create} from './create.function.js';
import {initializeApp} from 'firebase-admin/app';
import {userPath} from 'mirror-schema/src/user.js';
import {teamPath} from 'mirror-schema/src/team.js';
import {appPath} from 'mirror-schema/src/deployment.js';
import {appNameIndexPath} from 'mirror-schema/src/app.js';

mockFunctionParamsAndSecrets();

function callCreate(
  firestore: Firestore,
  userID: string,
  email: string,
  appName?: string,
) {
  const createFunction = https.onCall(create(firestore));

  return createFunction.run({
    data: {
      requester: {
        userID,
        userAgent: {type: 'reflect-cli', version: '0.0.1'},
      },
      serverReleaseChannel: 'stable',
      name: appName,
    },

    auth: {
      uid: userID,
      token: {email} as DecodedIdToken,
    },
    rawRequest: null as unknown as Request,
  });
}

describe('app-create function', () => {
  initializeApp({projectId: 'deploy-function-test'});
  const firestore = getFirestore();
  const USER_ID = 'app-create-test-user';
  const TEAM_ID = 'app-create-test-team';

  async function deleteApp(appID: string, appName: string): Promise<void> {
    const batch = firestore.batch();
    batch.delete(firestore.doc(appPath(appID)));
    batch.delete(firestore.doc(appNameIndexPath(appName)));
    await batch.commit();
  }

  // Clean up test data from global emulator state.
  afterEach(async () => {
    await firestore.runTransaction(async tx => {
      const docs = await tx.getAll(
        firestore.doc(userPath(USER_ID)),
        firestore.doc(teamPath(TEAM_ID)),
        firestore.doc(teamMembershipPath(TEAM_ID, USER_ID)),
      );
      for (const doc of docs) {
        tx.delete(doc.ref);
      }
    });
  });

  describe('create when user is already member of a team', () => {
    for (const role of ['admin', 'member'] as const) {
      for (const appName of [undefined, 'my-app'] as const) {
        test(`create with ${role} roll and appName ${appName}`, async () => {
          const email = 'foo@bar.com';
          const name = 'Test User';

          const user = await setUser(firestore, USER_ID, email, name, {
            [TEAM_ID]: role,
          });

          const team = await setTeam(firestore, TEAM_ID, {
            numAdmins: 1,
            maxApps: 5,
          });

          const teamMembership: Membership = await setMembership(
            firestore,
            TEAM_ID,
            USER_ID,
            email,
            role,
          );

          const resp = await callCreate(firestore, USER_ID, email, appName);
          expect(resp).toMatchObject({
            success: true,
            appID: expect.any(String),
            name: expect.any(String),
          });

          const newUser = await getUser(firestore, USER_ID);
          expect(newUser).toEqual(user);

          const newTeam = await getTeam(firestore, TEAM_ID);
          expect(newTeam).toEqual({
            ...team,
            numApps: 1,
          });

          const membership = await getMembership(firestore, TEAM_ID, USER_ID);
          expect(membership).toEqual(teamMembership);

          const app = await getApp(firestore, resp.appID);
          expect(app).toMatchObject({
            teamID: TEAM_ID,
            name: appName ?? expect.any(String),
            cfID: 'default-CLOUDFLARE_ACCOUNT_ID',
            cfScriptName: expect.any(String),
            serverReleaseChannel: 'stable',
            deploymentOptions: {
              vars: {
                /* eslint-disable @typescript-eslint/naming-convention */
                DISABLE_LOG_FILTERING: 'false',
                LOG_LEVEL: 'info',
                /* eslint-enable @typescript-eslint/naming-convention */
              },
            },
          });

          // Internal consistency checks.
          expect(app.name).toBe(resp.name);
          expect(app.cfScriptName.endsWith(`-${resp.appID}`)).toBe(true);

          const appNameEntry = await getAppName(firestore, app.name);
          expect(appNameEntry).toEqual({
            appID: resp.appID,
          });

          // Cleanup
          await deleteApp(resp.appID, resp.name);
        });
      }
    }
  });

  test('create when no team', async () => {
    const email = 'foo@bar.com';
    const user = await setUser(firestore, USER_ID, email, 'Foo Bar', {});

    const resp = await callCreate(firestore, USER_ID, email);

    expect(resp).toMatchObject({
      success: true,
      appID: expect.any(String),
      name: expect.any(String),
    });

    const newUser = await getUser(firestore, USER_ID);
    expect(Object.values(newUser.roles)).toEqual(['admin']);
    const teamID = Object.keys(newUser.roles)[0];
    expect(newUser).toEqual({
      ...user,
      roles: {[teamID]: 'admin'},
    });

    const team = await getTeam(firestore, teamID);
    expect(team).toEqual({
      name: '',
      defaultCfID: 'default-CLOUDFLARE_ACCOUNT_ID',
      numAdmins: 1,
      numMembers: 0,
      numInvites: 0,
      numApps: 1,
      maxApps: DEFAULT_MAX_APPS,
    });

    const membership = await getMembership(firestore, teamID, USER_ID);
    expect(membership).toEqual({
      email,
      role: 'admin',
    });

    const app = await getApp(firestore, resp.appID);
    expect(app).toMatchObject({
      teamID,
      name: expect.any(String),
      cfID: 'default-CLOUDFLARE_ACCOUNT_ID',
      cfScriptName: expect.any(String),
      serverReleaseChannel: 'stable',
      deploymentOptions: {
        vars: {
          /* eslint-disable @typescript-eslint/naming-convention */
          DISABLE_LOG_FILTERING: 'false',
          LOG_LEVEL: 'info',
          /* eslint-enable @typescript-eslint/naming-convention */
        },
      },
    });

    // Internal consistency checks.
    expect(app.name).toBe(resp.name);
    expect(app.cfScriptName.endsWith(`-${resp.appID}`)).toBe(true);

    const appName = await getAppName(firestore, app.name);
    expect(appName).toEqual({
      appID: resp.appID,
    });

    // Cleanup
    await deleteApp(resp.appID, resp.name);
  });

  test(`create when too many apps`, async () => {
    const email = 'foo@bar.com';
    const name = 'Test User';

    const user = await setUser(firestore, USER_ID, email, name, {
      [TEAM_ID]: 'admin',
    });

    const team = await setTeam(firestore, TEAM_ID, {
      numAdmins: 1,
      numApps: 5,
      maxApps: 5,
    });

    const teamMembership: Membership = await setMembership(
      firestore,
      TEAM_ID,
      USER_ID,
      email,
      'admin',
    );

    let error;
    try {
      await callCreate(firestore, USER_ID, email);
    } catch (e) {
      error = e;
    }
    expect(error).toBeInstanceOf(HttpsError);
    expect((error as HttpsError).message).toBe('Team has too many apps');

    const newUser = await getUser(firestore, USER_ID);
    expect(newUser).toEqual(user);

    const newTeam = await getTeam(firestore, TEAM_ID);
    expect(newTeam).toEqual(team);

    const membership = await getMembership(firestore, TEAM_ID, USER_ID);
    expect(membership).toEqual(teamMembership);
  });

  test(`create with invalid app name`, async () => {
    const email = 'foo@bar.com';
    const name = 'Test User';

    const user = await setUser(firestore, USER_ID, email, name, {
      [TEAM_ID]: 'admin',
    });

    const team = await setTeam(firestore, TEAM_ID, {
      numAdmins: 1,
      numApps: 1,
      maxApps: 5,
    });

    const teamMembership: Membership = await setMembership(
      firestore,
      TEAM_ID,
      USER_ID,
      email,
      'admin',
    );

    let error;
    try {
      await callCreate(firestore, USER_ID, email, 'Invalid-App-Name');
    } catch (e) {
      error = e;
    }
    expect(error).toBeInstanceOf(HttpsError);
    expect((error as HttpsError).message).toMatch('Invalid App Name');

    const newUser = await getUser(firestore, USER_ID);
    expect(newUser).toEqual(user);

    const newTeam = await getTeam(firestore, TEAM_ID);
    expect(newTeam).toEqual(team);

    const membership = await getMembership(firestore, TEAM_ID, USER_ID);
    expect(membership).toEqual(teamMembership);
  });
});
