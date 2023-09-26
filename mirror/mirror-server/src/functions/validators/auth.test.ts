import {describe, expect, test} from '@jest/globals';
import type {Firestore} from 'firebase-admin/firestore';
import {
  fakeFirestore,
  setApp,
  setUser,
} from 'mirror-schema/src/test-helpers.js';
import {https} from 'firebase-functions/v2';
import {
  FunctionsErrorCode,
  HttpsError,
  Request,
} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';
import {validateSchema} from './schema.js';
import {appAuthorization, userAuthorization} from './auth.js';
import {baseAppRequestFields} from 'mirror-protocol/src/app.js';
import {baseResponseFields} from 'mirror-protocol/src/base.js';
import * as v from 'shared/src/valita.js';
import type {Callable} from './types.js';
import type {User} from 'mirror-schema/src/user.js';
import type {App} from 'mirror-schema/src/app.js';
import type {Role} from 'mirror-schema/src/membership.js';
import {defaultOptions} from 'mirror-schema/src/deployment.js';
import {DEFAULT_PROVIDER_ID} from 'mirror-schema/src/provider.js';

const testRequestSchema = v.object({
  ...baseAppRequestFields,
  foo: v.string(),
});

const testResponseSchema = v.object({
  ...baseResponseFields,
  appName: v.string(),
  userEmail: v.string(),
  role: v.union(v.literal('admin'), v.literal('member')),
  bar: v.string(),
});

type TestRequest = v.Infer<typeof testRequestSchema>;
type TestResponse = v.Infer<typeof testResponseSchema>;

function testFunction(
  firestore: Firestore,
  allowedRoles: Role[] = ['admin', 'member'],
): Callable<TestRequest, TestResponse> {
  return validateSchema(testRequestSchema, testResponseSchema)
    .validate(userAuthorization())
    .validate(appAuthorization(firestore, allowedRoles))
    .handle(
      // eslint-disable-next-line require-await
      async (testRequest, context) => ({
        appName: context.app.name,
        userEmail: context.user.email,
        role: context.role,
        bar: testRequest.foo,
        success: true,
      }),
    );
}

describe('user authorization', () => {
  const goodRequest = {
    requester: {
      userID: 'foo',
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    foo: 'boo',
    appID: 'myApp',
  };

  type Case = {
    name: string;
    request: TestRequest;
    authData: AuthData;
    errorCode?: FunctionsErrorCode;
  };
  const cases: Case[] = [
    {
      name: 'successful authentication',
      authData: {uid: 'foo'} as AuthData,
      request: goodRequest,
    },
    {
      name: 'missing authentication',
      authData: {} as AuthData,
      request: goodRequest,
      errorCode: 'unauthenticated',
    },
    {
      name: 'wrong authenticated user',
      authData: {uid: 'bar'} as AuthData,
      request: goodRequest,
      errorCode: 'permission-denied',
    },
    {
      name: 'user with super powers',
      authData: {
        uid: 'bar',
        token: {superUntil: Date.now() + 10000},
      } as unknown as AuthData,
      request: goodRequest,
    },
    {
      name: 'user with expired super powers',
      authData: {
        uid: 'bar',
        token: {superUntil: Date.now() - 10000},
      } as unknown as AuthData,
      request: goodRequest,
      errorCode: 'permission-denied',
    },
    {
      name: 'bad request',
      authData: {uid: 'foo'} as AuthData,
      request: {not: 'a valid request'} as unknown as TestRequest,
      errorCode: 'invalid-argument',
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const firestore = fakeFirestore();
      await setUser(firestore, 'foo', 'foo@bar.com', 'Foo', {
        ['appTeam']: 'admin',
      });
      await setApp(firestore, 'myApp', {
        teamID: 'appTeam',
        name: 'My App Name',
      });

      const authenticatedFunction = https.onCall(testFunction(firestore));

      let error: HttpsError | undefined;
      let resp: TestResponse | undefined;
      try {
        resp = await authenticatedFunction.run({
          auth: c.authData,
          data: c.request,
          rawRequest: null as unknown as Request,
        });
      } catch (e) {
        expect(e).toBeInstanceOf(HttpsError);
        error = e as HttpsError;
      }

      expect(error?.code).toBe(c.errorCode);
      if (!c.errorCode) {
        expect(resp).toEqual({
          appName: 'My App Name',
          userEmail: 'foo@bar.com',
          role: 'admin',
          bar: 'boo',
          success: true,
        });
      }
    });
  }
});

describe('app authorization', () => {
  const req = {
    requester: {
      userID: 'foo',
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    foo: 'boo',
    appID: 'myApp',
  };
  const defaultApp: App = {
    teamID: 'myTeam',
    teamLabel: 'teamlabel',
    name: 'My App',
    cfID: 'deprecated',
    provider: DEFAULT_PROVIDER_ID,
    cfScriptName: 'cfScriptName',
    serverReleaseChannel: 'stable',
    deploymentOptions: defaultOptions(),
  };
  const defaultUser: User = {
    email: 'foo@bar.com',
    roles: {[defaultApp.teamID]: 'admin'},
  };

  type Case = {
    name: string;
    userDoc?: User;
    appDoc?: App;
    allowedRoles?: Role[];
    errorCode?: FunctionsErrorCode;
    response?: TestResponse;
  };
  const cases: Case[] = [
    {
      name: 'admin authorized',
      userDoc: defaultUser,
      appDoc: defaultApp,
      response: {
        appName: defaultApp.name,
        userEmail: defaultUser.email,
        role: 'admin',
        bar: req.foo,
        success: true,
      },
    },
    {
      name: 'member authorized',
      userDoc: {
        ...defaultUser,
        roles: {[defaultApp.teamID]: 'member'},
      },
      appDoc: defaultApp,
      response: {
        appName: defaultApp.name,
        userEmail: defaultUser.email,
        role: 'member',
        bar: req.foo,
        success: true,
      },
    },
    {
      name: 'member not authorized',
      userDoc: {
        ...defaultUser,
        roles: {[defaultApp.teamID]: 'member'},
      },
      allowedRoles: ['admin'],
      appDoc: defaultApp,
      errorCode: 'permission-denied',
    },
    {
      name: 'non-member not authorized',
      userDoc: {
        ...defaultUser,
        roles: {},
      },
      appDoc: defaultApp,
      errorCode: 'permission-denied',
    },
    {
      name: 'user not initialized',
      appDoc: defaultApp,
      errorCode: 'failed-precondition',
    },
    {
      name: 'app does not exist',
      userDoc: defaultUser,
      errorCode: 'not-found',
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const firestore = fakeFirestore();
      const authenticatedFunction = https.onCall(
        testFunction(firestore, c.allowedRoles ?? ['admin', 'member']),
      );

      if (c.userDoc) {
        await firestore.doc(`users/${req.requester.userID}`).set(c.userDoc);
      }
      if (c.appDoc) {
        await firestore.doc(`apps/${req.appID}`).set(c.appDoc);
      }

      let response: TestResponse | undefined;
      let error: HttpsError | undefined;
      try {
        response = await authenticatedFunction.run({
          auth: {uid: req.requester.userID} as AuthData,
          data: req,
          rawRequest: null as unknown as Request,
        });
      } catch (e) {
        expect(e).toBeInstanceOf(HttpsError);
        error = e as HttpsError;
      }

      expect(error?.code).toBe(c.errorCode);
      expect(response).toEqual(c.response);
    });
  }
});
