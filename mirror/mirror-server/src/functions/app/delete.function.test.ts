import {describe, expect, test, beforeEach, afterEach} from '@jest/globals';
import {
  setUser,
  setApp,
  setTeam,
  setMembership,
  getApp,
} from 'mirror-schema/src/test-helpers.js';
import {https} from 'firebase-functions/v2';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';
import type {DeleteAppRequest} from 'mirror-protocol/src/app.js';
import {teamPath} from 'mirror-schema/src/team.js';
import {initializeApp} from 'firebase-admin/app';
import {getFirestore} from 'firebase-admin/firestore';
import {
  appPath,
  deploymentDataConverter,
  deploymentsCollection,
} from 'mirror-schema/src/deployment.js';
import {deleteApp} from './delete.function.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {teamMembershipPath} from 'mirror-schema/src/membership.js';
import {fail} from 'assert';

describe('app-delete function', () => {
  initializeApp({projectId: 'delete-function-test'});
  const firestore = getFirestore();
  const APP_ID = 'app-delete-test-app';
  const TEAM_ID = 'app-delete-test-team';
  const USER_ID = 'app-delete-test-user';

  const deleteFunction = https.onCall(deleteApp(firestore));

  const request: DeleteAppRequest = {
    requester: {
      userAgent: {
        type: 'reflect-cli',
        version: '0.0.1',
      },
      userID: USER_ID,
    },
    appID: APP_ID,
  } as const;

  beforeEach(async () => {
    await setUser(firestore, USER_ID, 'foo@bar.com', undefined, {
      [TEAM_ID]: 'admin',
    });
    await setTeam(firestore, TEAM_ID, {});
    await setMembership(firestore, TEAM_ID, USER_ID, 'foo@bar.com', 'admin');
    await setApp(firestore, APP_ID, {teamID: TEAM_ID});
  });

  afterEach(async () => {
    const batch = firestore.batch();
    const deployments = await firestore
      .collection(deploymentsCollection(APP_ID))
      .listDocuments();
    deployments.forEach(doc => batch.delete(doc));
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(userPath(USER_ID)));
    batch.delete(firestore.doc(teamPath(TEAM_ID)));
    batch.delete(firestore.doc(teamMembershipPath(TEAM_ID, USER_ID)));
    await batch.commit();
  });

  test('requests delete deployment', async () => {
    const resp = await deleteFunction.run({
      auth: {uid: USER_ID} as AuthData,
      data: request,
      rawRequest: null as unknown as Request,
    });

    const {deploymentPath} = resp;
    const deploymentDoc = await firestore
      .doc(deploymentPath)
      .withConverter(deploymentDataConverter);
    const deployment = await deploymentDoc.get();
    expect(deployment.exists).toBe(true);
    expect(deployment.data()).toMatchObject({
      type: 'DELETE',
      requesterID: USER_ID,
      status: 'REQUESTED',
    });

    const app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toEqual([deployment.id]);
  });

  test('rejects non-admin', async () => {
    await firestore
      .doc(userPath(USER_ID))
      .withConverter(userDataConverter)
      .update({
        roles: {[TEAM_ID]: 'member'},
      });

    try {
      await deleteFunction.run({
        auth: {uid: USER_ID} as AuthData,
        data: request,
        rawRequest: null as unknown as Request,
      });
      fail('app-delete should not succeed for non-admin');
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('permission-denied');
    }

    const app = await getApp(firestore, APP_ID);
    expect(app.queuedDeploymentIDs).toBeUndefined;
  });
});
