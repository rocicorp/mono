import {describe, expect, test, beforeEach, afterEach} from '@jest/globals';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import type {Request} from 'firebase-functions/v2/https';
import {teamMembershipPath} from 'mirror-schema/src/membership.js';
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
  teamLabelIndexDataConverter,
  teamLabelIndexPath,
} from 'mirror-schema/src/team.js';

mockFunctionParamsAndSecrets();

describe('team-ensure function', () => {
  initializeApp({projectId: 'team-ensure-function-test'});
  const firestore = getFirestore();
  const USER_ID = 'team-ensure-test-user';
  const USER_EMAIL = 'foo@bar.com';
  const TEAM_ID = 'team-ensure-test-team';

  function callEnsure(name: string) {
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

  async function deleteTeam(teamID: string, label: string): Promise<void> {
    const batch = firestore.batch();
    batch.delete(firestore.doc(teamPath(teamID)));
    batch.delete(firestore.doc(teamLabelIndexPath(label)));
    batch.delete(firestore.doc(teamMembershipPath(teamID, USER_ID)));
    await batch.commit();
  }

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

        const resp = await callEnsure('ignored');
        expect(resp).toEqual({
          success: true,
          teamID: TEAM_ID,
        });

        // Only the user doc is checked; no other actions should have been performed.
        const teamDoc = await firestore.doc(teamPath(TEAM_ID)).get();
        expect(teamDoc.exists).toBe(false);
      });
    }
  });

  test('ensure when no team', async () => {
    const resp = await callEnsure('My Team, LLC.');
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
      label: 'myteamllc',
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
    const {label} = team;
    const labelIndex = await firestore.doc(teamLabelIndexPath(label)).get();
    expect(labelIndex.data()).toEqual({teamID});

    // Cleanup
    await deleteTeam(teamID, label);
  });

  test('ensure team with colliding label', async () => {
    await firestore
      .doc(teamLabelIndexPath('existingteamnamellc'))
      .withConverter(teamLabelIndexDataConverter)
      .create({
        teamID: TEAM_ID,
      });

    const resp = await callEnsure('Existing Team Name, LLC.');
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
      label: expect.stringMatching(/existingteamnamellc\d+/),
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
    const {label} = team;
    const labelIndex = await firestore.doc(teamLabelIndexPath(label)).get();
    expect(labelIndex.data()).toEqual({teamID});

    // Cleanup
    await deleteTeam(teamID, label);
    await deleteTeam(TEAM_ID, 'existingteamnamellc');
  });
});
