import {describe, expect, test} from '@jest/globals';
import type {DecodedIdToken} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import type {Membership} from 'mirror-schema/src/membership.js';
import {
  fakeFirestore,
  getApp,
  getMembership,
  getTeam,
  getUser,
  setMembership,
  setTeam,
  setUser,
} from 'mirror-schema/src/test-helpers.js';
import {installCrypto, mockCloudflareStringParam} from '../../test-helpers.js';
import {DEFAULT_MAX_APPS, create} from './create.function.js';

mockCloudflareStringParam();
await installCrypto();

function callCreate(firestore: Firestore, userID: string, email: string) {
  const createFunction = https.onCall(create(firestore));

  return createFunction.run({
    data: {
      requester: {
        userID,
        userAgent: {type: 'reflect-cli', version: '0.0.1'},
      },
      serverReleaseChannel: 'stable',
    },

    auth: {
      uid: userID,
      token: {email} as DecodedIdToken,
    },
    rawRequest: null as unknown as Request,
  });
}

describe('create when user is already member of a team', () => {
  for (const role of ['admin', 'member'] as const) {
    test(`create when role was ${role}`, async () => {
      const firestore = fakeFirestore();

      const userID = 'foo';
      const teamID = 'fooTeam';
      const email = 'foo@bar.com';
      const name = 'Test User';

      const user = await setUser(firestore, userID, email, name, {
        [teamID]: role,
      });

      // Make sure to set team before membership to not trigger a bug in
      // firestore-jest-mock.
      // https://github.com/Upstatement/firestore-jest-mock/issues/170
      const team = await setTeam(firestore, teamID, {
        numAdmins: 1,
        maxApps: 5,
      });

      const teamMembership: Membership = await setMembership(
        firestore,
        teamID,
        userID,
        email,
        role,
      );

      const resp = await callCreate(firestore, userID, email);
      expect(resp).toMatchObject({
        success: true,
        appID: expect.any(String),
        name: expect.any(String),
      });

      const newUser = await getUser(firestore, userID);
      expect(newUser).toEqual(user);

      const newTeam = await getTeam(firestore, teamID);
      expect(newTeam).toEqual({
        ...team,
        numApps: 1,
      });

      const membership = await getMembership(firestore, teamID, userID);
      expect(membership).toEqual(teamMembership);

      const app = await getApp(firestore, resp.appID);
      expect(app).toMatchObject({
        teamID,
        name: expect.any(String),
        cfID: 'default-cloudflare-id',
        cfScriptName: expect.any(String),
        serverReleaseChannel: 'stable',
      });
    });
  }
});

test('create when no team', async () => {
  const firestore = fakeFirestore();
  const userID = 'foo';
  const email = 'foo@bar.com';
  const user = await setUser(firestore, userID, email, 'Foo Bar', {});

  const resp = await callCreate(firestore, userID, email);

  expect(resp).toMatchObject({
    success: true,
    appID: expect.any(String),
    name: expect.any(String),
  });

  const newUser = await getUser(firestore, userID);
  expect(Object.values(newUser.roles)).toEqual(['admin']);
  const teamID = Object.keys(newUser.roles)[0];
  expect(newUser).toEqual({
    ...user,
    roles: {[teamID]: 'admin'},
  });

  const team = await getTeam(firestore, teamID);
  expect(team).toEqual({
    name: '',
    defaultCfID: 'default-cloudflare-id',
    numAdmins: 1,
    numMembers: 0,
    numInvites: 0,
    numApps: 1,
    maxApps: DEFAULT_MAX_APPS,
  });

  const membership = await getMembership(firestore, teamID, userID);
  expect(membership).toEqual({
    email,
    role: 'admin',
  });

  const app = await getApp(firestore, resp.appID);
  expect(app).toMatchObject({
    teamID,
    name: expect.any(String),
    cfID: 'default-cloudflare-id',
    cfScriptName: expect.any(String),
    serverReleaseChannel: 'stable',
  });
});

test(`create when too many apps`, async () => {
  const firestore = fakeFirestore();

  const userID = 'foo';
  const teamID = 'fooTeam';
  const email = 'foo@bar.com';
  const name = 'Test User';

  const user = await setUser(firestore, userID, email, name, {
    [teamID]: 'admin',
  });

  // Make sure to set team before membership to not trigger a bug in
  // firestore-jest-mock.
  // https://github.com/Upstatement/firestore-jest-mock/issues/170
  const team = await setTeam(firestore, teamID, {
    numAdmins: 1,
    numApps: 5,
    maxApps: 5,
  });

  const teamMembership: Membership = await setMembership(
    firestore,
    teamID,
    userID,
    email,
    'admin',
  );

  let error;
  try {
    await callCreate(firestore, userID, email);
  } catch (e) {
    error = e;
  }
  expect(error).toBeInstanceOf(HttpsError);
  expect((error as HttpsError).message).toBe('Team has too many apps');

  const newUser = await getUser(firestore, userID);
  expect(newUser).toEqual(user);

  const newTeam = await getTeam(firestore, teamID);
  expect(newTeam).toEqual(team);

  const membership = await getMembership(firestore, teamID, userID);
  expect(membership).toEqual(teamMembership);
});
