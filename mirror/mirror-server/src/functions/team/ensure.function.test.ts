import {
  describe,
  expect,
  test,
  beforeAll,
  beforeEach,
  afterEach,
} from '@jest/globals';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {getFirestore, type Firestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import type {Request} from 'firebase-functions/v2/https';
import {
  TEAM_MEMBERSHIPS_COLLECTION_ID,
  membershipDataConverter,
  teamMembershipPath,
} from 'mirror-schema/src/membership.js';
import {
  getMembership,
  getTeam,
  getUser,
  setUser,
} from 'mirror-schema/src/test-helpers.js';
import {mockFunctionParamsAndSecrets} from '../../test-helpers.js';
import {DEFAULT_MAX_APPS, ensure} from './ensure.function.js';
import {initializeApp} from 'firebase-admin/app';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {
  teamPath,
  teamSubdomainIndexDataConverter,
  teamSubdomainIndexPath,
} from 'mirror-schema/src/team.js';
import {must} from 'shared/src/must.js';

mockFunctionParamsAndSecrets();

describe('team-ensure function', () => {
  initializeApp({projectId: 'team-ensure-function-test'});
  const firestore = getFirestore();
  const USER_ID = 'team-ensure-test-user';
  const USER_EMAIL = 'foo@bar.com';
  const TEAM_ID = 'team-ensure-test-team';

  function callEnsure(firestore: Firestore, name: string) {
    const ensureFunction = https.onCall(ensure(firestore));

    return ensureFunction.run({
      data: {
        requester: {
          userID: USER_ID,
          userAgent: {type: 'reflect-cli', version: '0.0.1'},
        },
        name,
      },

      auth: {
        uid: USER_ID,
        token: {email: USER_EMAIL} as DecodedIdToken,
      },
      rawRequest: null as unknown as Request,
    });
  }

  async function deleteTeam(teamID: string, subdomain: string): Promise<void> {
    const batch = firestore.batch();
    batch.delete(firestore.doc(teamPath(teamID)));
    batch.delete(firestore.doc(teamSubdomainIndexPath(subdomain)));
    batch.delete(firestore.doc(teamMembershipPath(teamID, USER_ID)));
    await batch.commit();
  }

  beforeAll(async () => {
    const subs = await firestore
      .collection('teamSubdomains')
      .withConverter(teamSubdomainIndexDataConverter)
      .get();
    for (const doc of subs.docs) {
      const team = await firestore.doc(teamPath(doc.data().teamID)).get();
      console.warn(
        `Existing subdomain: ${doc.ref.path}: ${doc.data().teamID}`,
        team.data(),
      );
      const members = await firestore
        .collection(TEAM_MEMBERSHIPS_COLLECTION_ID)
        .withConverter(membershipDataConverter)
        .get();
      console.warn(`Members: [${members.docs.map(doc => doc.data())}]`);
    }
  });

  beforeEach(async () => {
    const batch = firestore.batch();
    batch.create(
      firestore.doc(userPath(USER_ID)).withConverter(userDataConverter),
      {email: USER_EMAIL, roles: {}},
    );
    await batch.commit();
  });

  // Clean up test data from global emulator state.
  afterEach(async () => {
    const batch = firestore.batch();
    batch.delete(firestore.doc(userPath(USER_ID)));
    await batch.commit();
  });

  describe('ensure when user is already member of a team', () => {
    for (const role of ['admin', 'member'] as const) {
      test(`ensure with ${role} role`, async () => {
        const name = 'Test User';

        await setUser(firestore, USER_ID, USER_EMAIL, name, {
          [TEAM_ID]: role,
        });

        const resp = await callEnsure(firestore, 'ignored');
        expect(resp).toEqual({
          success: true,
          teamID: TEAM_ID,
        });
      });
    }
  });

  test('ensure when no team', async () => {
    const resp = await callEnsure(firestore, 'My Team, LLC.');
    expect(resp).toMatchObject({
      success: true,
      teamID: expect.any(String),
    });
    const {teamID} = resp;
    const user = await getUser(firestore, USER_ID);
    expect(user.roles).toEqual({[teamID]: 'admin'});
    const team = await getTeam(firestore, teamID);

    expect(team).toEqual({
      name: 'My Team, LLC.',
      subdomain: 'my-team-llc',
      defaultCfID: 'default-CLOUDFLARE_ACCOUNT_ID',
      numAdmins: 1,
      numMembers: 0,
      numInvites: 0,
      numApps: 0,
      maxApps: DEFAULT_MAX_APPS,
    });
    const membership = await getMembership(firestore, teamID, USER_ID);
    expect(membership).toEqual({
      email: USER_EMAIL,
      role: 'admin',
    });
    const subdomain = must(team.subdomain);
    const subdomainIndex = await firestore
      .doc(teamSubdomainIndexPath(subdomain))
      .get();
    expect(subdomainIndex.data()).toEqual({teamID});

    // Cleanup
    await deleteTeam(teamID, subdomain);
  });

  test('ensure team with colliding subdomain', async () => {
    await firestore
      .doc(teamSubdomainIndexPath('existing-team-name-llc'))
      .withConverter(teamSubdomainIndexDataConverter)
      .create({
        teamID: TEAM_ID,
      });

    const resp = await callEnsure(firestore, 'Existing Team Name, LLC.');
    expect(resp).toMatchObject({
      success: true,
      teamID: expect.any(String),
    });
    const {teamID} = resp;
    const user = await getUser(firestore, USER_ID);
    expect(user.roles).toEqual({[teamID]: 'admin'});
    const team = await getTeam(firestore, teamID);

    expect(team).toEqual({
      name: 'Existing Team Name, LLC.',
      subdomain: expect.stringMatching(/existing-team-name-llc-\d+/),
      defaultCfID: 'default-CLOUDFLARE_ACCOUNT_ID',
      numAdmins: 1,
      numMembers: 0,
      numInvites: 0,
      numApps: 0,
      maxApps: DEFAULT_MAX_APPS,
    });
    const membership = await getMembership(firestore, teamID, USER_ID);
    expect(membership).toEqual({
      email: USER_EMAIL,
      role: 'admin',
    });
    const subdomain = must(team.subdomain);
    const subdomainIndex = await firestore
      .doc(teamSubdomainIndexPath(subdomain))
      .get();
    expect(subdomainIndex.data()).toEqual({teamID});

    // Cleanup
    await deleteTeam(teamID, subdomain);
    await deleteTeam(TEAM_ID, 'existing-team-name-llc');
  });
});
