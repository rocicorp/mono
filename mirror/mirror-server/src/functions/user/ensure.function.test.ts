import {describe, expect, test} from '@jest/globals';
import type {Auth} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import {
  FunctionsErrorCode,
  HttpsError,
  Request,
} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';
import {firebaseStub} from 'firestore-jest-mock/mocks/firebase.js';
import type {EnsureUserRequest} from 'mirror-protocol/src/user.js';
import {ensure} from './ensure.function.js';

function fakeFirestore(): Firestore {
  return firebaseStub(
    {database: {}},
    {mutable: true},
  ).firestore() as unknown as Firestore;
}

function fakeAuth(email = 'foo@bar.com'): Auth {
  const auth = {
    getUser: () => Promise.resolve({email}),
    createCustomToken: () => Promise.resolve('custom-auth-token'),
  };
  return auth as unknown as Auth;
}

describe('request validation', () => {
  const goodRequest = {
    requester: {
      userID: 'foo',
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
  };

  type Case = {
    name: string;
    request: EnsureUserRequest;
    authData: AuthData;
    auth?: Auth;
    errorCode: FunctionsErrorCode;
  };
  const cases: Case[] = [
    {
      name: 'missing authentication',
      authData: {} as AuthData,
      request: goodRequest,
      errorCode: 'unauthenticated',
    },
    {
      name: 'missing email',
      authData: {uid: 'foo'} as AuthData,
      auth: fakeAuth(''),
      request: goodRequest,
      errorCode: 'failed-precondition',
    },
    {
      name: 'wrong authenticated user',
      authData: {uid: 'bar'} as AuthData,
      request: goodRequest,
      errorCode: 'permission-denied',
    },
    {
      name: 'bad request',
      authData: {uid: 'foo'} as AuthData,
      request: {not: 'a valid request'} as unknown as EnsureUserRequest,
      errorCode: 'invalid-argument',
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const firestore = fakeFirestore();
      const ensureFunction = https.onCall(
        ensure(firestore, c.auth ?? fakeAuth()),
      );

      let error: HttpsError | undefined = undefined;
      try {
        await ensureFunction.run({
          auth: c.authData,
          data: c.request,
          rawRequest: null as unknown as Request,
        });
      } catch (e) {
        expect(e).toBeInstanceOf(HttpsError);
        error = e as HttpsError;
      }

      expect(error?.code).toBe(c.errorCode);
      const fooDoc = await firestore.doc('users/foo').get();
      expect(fooDoc.exists).toBe(false);
    });
  }
});

test('creates user doc', async () => {
  const firestore = fakeFirestore();
  const ensureFunction = https.onCall(ensure(firestore, fakeAuth()));

  const resp = await ensureFunction.run({
    data: {
      requester: {
        userID: 'foo',
        userAgent: {type: 'reflect-cli', version: '0.0.1'},
      },
    },
    auth: {uid: 'foo'} as AuthData,
    rawRequest: null as unknown as Request,
  });
  expect(resp).toEqual({customToken: 'custom-auth-token', success: true});
  const fooDoc = await firestore.doc('users/foo').get();
  expect(fooDoc.exists).toBe(true);
  expect(fooDoc.data()).toEqual({
    email: 'foo@bar.com',
    roles: {},
  });
});

test('does not overwrite existing user doc', async () => {
  const firestore = fakeFirestore();
  const ensureFunction = https.onCall(ensure(firestore, fakeAuth()));

  await firestore.doc('users/foo').set({
    email: 'foo@bar.com',
    name: 'Foo Bar',
    roles: {fooTeam: 'a'},
  });

  const resp = await ensureFunction.run({
    data: {
      requester: {
        userID: 'foo',
        userAgent: {type: 'reflect-cli', version: '0.0.1'},
      },
    },
    auth: {uid: 'foo'} as AuthData,
    rawRequest: null as unknown as Request,
  });
  expect(resp).toEqual({customToken: 'custom-auth-token', success: true});
  const fooDoc = await firestore.doc('users/foo').get();
  expect(fooDoc.exists).toBe(true);
  expect(fooDoc.data()).toEqual({
    email: 'foo@bar.com',
    name: 'Foo Bar',
    roles: {fooTeam: 'a'},
  });
});

test('updates user doc if email is different', async () => {
  const firestore = fakeFirestore();
  const ensureFunction = https.onCall(
    ensure(firestore, fakeAuth('new@email-address.com')),
  );

  await firestore.doc('users/foo').set({
    email: 'old@email-address.com',
    name: 'Foo Bar',
    roles: {fooTeam: 'a'},
    invites: {barTeam: 'm'},
  });
  await firestore.doc('teams/fooTeam/memberships/foo').set({
    email: 'old@email-address.com',
    role: 'admin',
  });
  await firestore.doc('teams/barTeam/invites/foo').set({
    email: 'old@email-address.com',
    role: 'member',
  });

  const resp = await ensureFunction.run({
    data: {
      requester: {
        userID: 'foo',
        userAgent: {type: 'reflect-cli', version: '0.0.1'},
      },
    },
    auth: {uid: 'foo'} as AuthData,
    rawRequest: null as unknown as Request,
  });
  expect(resp).toEqual({customToken: 'custom-auth-token', success: true});
  const fooDoc = await firestore.doc('users/foo').get();
  expect(fooDoc.exists).toBe(true);
  expect(fooDoc.data()).toEqual({
    email: 'new@email-address.com',
    name: 'Foo Bar',
    roles: {fooTeam: 'a'},
    invites: {barTeam: 'm'},
  });
  const fooTeamMembershipDoc = await firestore
    .doc('teams/fooTeam/memberships/foo')
    .get();
  expect(fooTeamMembershipDoc.exists).toBe(true);
  expect(fooTeamMembershipDoc.data()).toEqual({
    email: 'new@email-address.com',
    role: 'admin',
  });
  const barTeamMembershipDoc = await firestore
    .doc('teams/barTeam/invites/foo')
    .get();
  expect(barTeamMembershipDoc.exists).toBe(true);
  expect(barTeamMembershipDoc.data()).toEqual({
    email: 'new@email-address.com',
    role: 'member',
  });
});
