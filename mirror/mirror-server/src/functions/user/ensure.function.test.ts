import {describe, expect, test} from '@jest/globals';
import type {DecodedIdToken} from 'firebase-admin/auth';
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
    auth: AuthData;
    errorCode: FunctionsErrorCode;
  };
  const cases: Case[] = [
    {
      name: 'missing authentication',
      auth: {} as AuthData,
      request: goodRequest,
      errorCode: 'unauthenticated',
    },
    {
      name: 'missing email',
      auth: {
        uid: 'foo',
        token: {
          /* no email field */
        } as DecodedIdToken,
      },
      request: goodRequest,
      errorCode: 'failed-precondition',
    },
    {
      name: 'wrong authenticated user',
      auth: {
        uid: 'bar',
        token: {email: 'foo@bar.com'},
      } as AuthData,
      request: goodRequest,
      errorCode: 'permission-denied',
    },
    {
      name: 'bad request',
      auth: {
        uid: 'foo',
        token: {email: 'foo@bar.com'},
      } as AuthData,
      request: {not: 'a valid request'} as unknown as EnsureUserRequest,
      errorCode: 'invalid-argument',
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const firestore = fakeFirestore();
      const ensureFunction = https.onCall(ensure(firestore));

      let error: HttpsError | undefined = undefined;
      try {
        await ensureFunction.run({
          auth: c.auth,
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
  const ensureFunction = https.onCall(ensure(firestore));

  const resp = await ensureFunction.run({
    data: {
      requester: {
        userID: 'foo',
        userAgent: {type: 'reflect-cli', version: '0.0.1'},
      },
    },
    auth: {
      uid: 'foo',
      token: {email: 'foo@bar.com'} as DecodedIdToken,
    },
    rawRequest: null as unknown as Request,
  });
  expect(resp).toEqual({success: true});
  const fooDoc = await firestore.doc('users/foo').get();
  expect(fooDoc.exists).toBe(true);
  expect(fooDoc.data()).toEqual({
    email: 'foo@bar.com',
    roles: {},
  });
});

test('does not overwrite existing user doc', async () => {
  const firestore = fakeFirestore();
  const ensureFunction = https.onCall(ensure(firestore));

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
    auth: {
      uid: 'foo',
      token: {email: 'foo@bar.com'} as DecodedIdToken,
    },
    rawRequest: null as unknown as Request,
  });
  expect(resp).toEqual({success: true});
  const fooDoc = await firestore.doc('users/foo').get();
  expect(fooDoc.exists).toBe(true);
  expect(fooDoc.data()).toEqual({
    email: 'foo@bar.com',
    name: 'Foo Bar',
    roles: {fooTeam: 'a'},
  });
});
