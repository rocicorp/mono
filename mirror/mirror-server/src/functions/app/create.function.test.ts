import {describe, expect, test} from '@jest/globals';
import type {DecodedIdToken} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {declaredParams} from 'firebase-functions/params';
import {https} from 'firebase-functions/v2';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import {firebaseStub} from 'firestore-jest-mock/mocks/firebase.js';
import {App, appDataConverter, appPath} from 'mirror-schema/src/app.js';
import {
  Membership,
  Role,
  membershipDataConverter,
  teamMembershipPath,
  type ShortRole,
} from 'mirror-schema/src/membership.js';
import {
  teamDataConverter,
  teamPath,
  type Team,
} from 'mirror-schema/src/team.js';
import {
  userDataConverter,
  userPath,
  type User,
} from 'mirror-schema/src/user.js';
import {must} from 'shared/src/must.js';
import {DEFAULT_MAX_APPS, create} from './create.function.js';

function mockStringParam() {
  for (const p of declaredParams) {
    if (p.name === 'CLOUDFLARE_ACCOUNT_ID') {
      p.value = p.toString = () => 'default-cloudflare-id';
    }
  }
}

mockStringParam();

function fakeFirestore(): Firestore {
  return firebaseStub(
    {database: {}},
    {mutable: true},
  ).firestore() as unknown as Firestore;
}

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

async function setUser(
  firestore: Firestore,
  userID: string,
  email: string,
  name = 'Foo Bar',
  roles: Record<string, ShortRole> = {},
): Promise<User> {
  const user: User = {
    email,
    name,
    roles,
  };
  await firestore
    .doc(userPath(userID))
    .withConverter(userDataConverter)
    .set(user);
  return user;
}

async function getUser(firestore: Firestore, userID: string): Promise<User> {
  const userDoc = await firestore
    .doc(userPath(userID))
    .withConverter(userDataConverter)
    .get();
  return must(userDoc.data());
}

async function setTeam(
  firestore: Firestore,
  teamID: string,
  team: Partial<Team>,
): Promise<Team> {
  const {
    name = `Name of ${teamID}`,
    defaultCfID = 'default-cloudflare-id',
    admins = [],
    members = [],
    numApps = 0,
    maxApps = null,
  } = team;
  const newTeam: Team = {
    name,
    defaultCfID,
    admins,
    members,
    numApps,
    maxApps,
  };
  await firestore
    .doc(teamPath(teamID))
    .withConverter(teamDataConverter)
    // Work around bug in
    .set(newTeam, {merge: true});
  return newTeam;
}

async function getTeam(firestore: Firestore, teamID: string): Promise<Team> {
  const teamDoc = await firestore
    .doc(teamPath(teamID))
    .withConverter(teamDataConverter)
    .get();
  return must(teamDoc.data());
}

async function setTeamMembership(
  firestore: Firestore,
  teamID: string,
  userID: string,
  email: string,
  role: Role,
): Promise<Membership> {
  const membership: Membership = {
    email,
    role,
  };
  await firestore
    .doc(teamMembershipPath(teamID, userID))
    .withConverter(membershipDataConverter)
    .set(membership);
  return membership;
}

async function getMembership(
  firestore: Firestore,
  teamID: string,
  userID: string,
): Promise<Membership> {
  const membershipDoc = await firestore
    .doc(teamMembershipPath(teamID, userID))
    .withConverter(membershipDataConverter)
    .get();
  return must(membershipDoc.data());
}

async function getApp(firestore: Firestore, appID: string): Promise<App> {
  const appDoc = await firestore
    .doc(appPath(appID))
    .withConverter(appDataConverter)
    .get();
  return must(appDoc.data());
}

describe('create when user is already member of a team', () => {
  for (const role of ['admin', 'member'] as const) {
    const shortRole: ShortRole = role === 'admin' ? 'a' : 'm';
    test(`create when role was ${role}`, async () => {
      const firestore = fakeFirestore();

      const userID = 'foo';
      const teamID = 'fooTeam';
      const email = 'foo@bar.com';
      const name = 'Test User';

      const user = await setUser(firestore, userID, email, name, {
        [teamID]: shortRole,
      });

      // Make sure to set team before membership to not trigger a bug in
      // firestore-jest-mock.
      // https://github.com/Upstatement/firestore-jest-mock/issues/170
      const team = await setTeam(firestore, teamID, {
        admins: [userID],
        maxApps: 5,
      });

      const teamMembership: Membership = await setTeamMembership(
        firestore,
        teamID,
        userID,
        email,
        role,
      );

      const resp = await callCreate(firestore, userID, email);
      expect(resp).toMatchObject({success: true, appID: expect.any(String)});

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

  expect(resp).toMatchObject({success: true, appID: expect.any(String)});

  const newUser = await getUser(firestore, userID);
  expect(Object.values(newUser.roles)).toEqual(['a']);
  const teamID = Object.keys(newUser.roles)[0];
  expect(newUser).toEqual({
    ...user,
    roles: {[teamID]: 'a'},
  });

  const team = await getTeam(firestore, teamID);
  expect(team).toEqual({
    name: '',
    defaultCfID: 'default-cloudflare-id',
    admins: [userID],
    members: [],
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
    [teamID]: 'a',
  });

  // Make sure to set team before membership to not trigger a bug in
  // firestore-jest-mock.
  // https://github.com/Upstatement/firestore-jest-mock/issues/170
  const team = await setTeam(firestore, teamID, {
    admins: [userID],
    numApps: 5,
    maxApps: 5,
  });

  const teamMembership: Membership = await setTeamMembership(
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
