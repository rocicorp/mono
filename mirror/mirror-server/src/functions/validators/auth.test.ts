import {afterEach, describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import {Timestamp, getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import {
  FunctionsErrorCode,
  HttpsError,
  Request,
} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';
import {baseAppRequestFields} from 'mirror-protocol/src/app.js';
import {baseResponseFields} from 'mirror-protocol/src/base.js';
import type {
  AppKey,
  Permissions,
  RequiredPermission,
} from 'mirror-schema/src/app-key.js';
import {appKeyPath} from 'mirror-schema/src/app-key.js';
import {appPath, type App} from 'mirror-schema/src/app.js';
import type {Role} from 'mirror-schema/src/membership.js';
import {DEFAULT_PROVIDER_ID} from 'mirror-schema/src/provider.js';
import {setApp, setUser} from 'mirror-schema/src/test-helpers.js';
import type {User} from 'mirror-schema/src/user.js';
import {userPath} from 'mirror-schema/src/user.js';
import * as v from 'shared/src/valita.js';
import {
  appAuthorization,
  appOrKeyAuthorization,
  userAuthorization,
  userOrKeyAuthorization,
} from './auth.js';
import {validateSchema} from './schema.js';
import type {Callable} from './types.js';

const testRequestSchema = v.object({
  ...baseAppRequestFields,
  foo: v.string(),
});

const testResponseSchema = v.object({
  ...baseResponseFields,
  appName: v.string(),
  bar: v.string(),
});

type TestRequest = v.Infer<typeof testRequestSchema>;
type TestResponse = v.Infer<typeof testResponseSchema>;

describe('auth-validators', () => {
  initializeApp({projectId: 'auth-validator-test'});
  const firestore = getFirestore();
  const USER_ID = 'auth-user-id';
  const APP_ID = 'auth-app-id';
  const APP_KEY_NAME = 'auth-app-key';

  afterEach(async () => {
    const batch = firestore.batch();
    batch.delete(firestore.doc(userPath(USER_ID)));
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(appKeyPath(APP_ID, APP_KEY_NAME)));
    await batch.commit();
  });

  function testFunction(
    allowedRoles: Role[] = ['admin', 'member'],
  ): Callable<TestRequest, TestResponse> {
    return validateSchema(testRequestSchema, testResponseSchema)
      .validate(userAuthorization())
      .validate(appAuthorization(firestore, allowedRoles))
      .handle(
        // eslint-disable-next-line require-await
        async (testRequest, context) => ({
          appName: context.app.name,
          bar: testRequest.foo,
          success: true,
        }),
      );
  }

  function testFunctionWithKeys(
    keyPermission: RequiredPermission,
  ): Callable<TestRequest, TestResponse> {
    return validateSchema(testRequestSchema, testResponseSchema)
      .validate(userOrKeyAuthorization())
      .validate(appOrKeyAuthorization(firestore, keyPermission))
      .handle(
        // eslint-disable-next-line require-await
        async (testRequest, context) => ({
          appName: context.app.name,
          bar: testRequest.foo,
          success: true,
        }),
      );
  }

  const goodRequest = {
    requester: {
      userID: USER_ID,
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    foo: 'boo',
    appID: APP_ID,
  };

  type UserCase = {
    name: string;
    request: TestRequest;
    authData: AuthData;
    errorCode?: FunctionsErrorCode;
  };
  const userCases: UserCase[] = [
    {
      name: 'successful authentication',
      authData: {uid: USER_ID} as AuthData,
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
      authData: {uid: USER_ID} as AuthData,
      request: {not: 'a valid request'} as unknown as TestRequest,
      errorCode: 'invalid-argument',
    },
  ];

  for (const c of userCases) {
    test(`user authentication / ${c.name}`, async () => {
      await setUser(firestore, USER_ID, 'foo@bar.com', USER_ID, {
        ['appTeam']: 'admin',
      });
      await setApp(firestore, APP_ID, {
        teamID: 'appTeam',
        name: 'My App Name',
      });

      const authenticatedFunction = https.onCall(testFunction());

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
          bar: 'boo',
          success: true,
        });
      }
    });
  }

  const defaultApp: App = {
    teamID: 'myTeam',
    teamLabel: 'teamlabel',
    name: 'My App',
    cfID: 'deprecated',
    provider: DEFAULT_PROVIDER_ID,
    cfScriptName: 'cfScriptName',
    serverReleaseChannel: 'stable',
    envUpdateTime: Timestamp.now(),
  };
  const defaultUser: User = {
    email: 'foo@bar.com',
    roles: {[defaultApp.teamID]: 'admin'},
  };

  const appReq = {
    requester: {
      userID: USER_ID,
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    foo: 'boo',
    appID: APP_ID,
  };

  const appKeyReq = {
    requester: {
      userID: `apps/${APP_ID}/keys/${APP_KEY_NAME}`,
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    foo: 'boo',
    appID: APP_ID,
  };

  type AppCase = {
    name: string;
    request?: TestRequest;
    userDoc?: User;
    appDoc?: App;
    allowedRoles?: Role[];
    errorCode?: FunctionsErrorCode;
    response?: TestResponse;
  };
  const appCases: AppCase[] = [
    {
      name: 'admin authorized',
      userDoc: defaultUser,
      appDoc: defaultApp,
      response: {
        appName: defaultApp.name,
        bar: appReq.foo,
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
        bar: appReq.foo,
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
    {
      name: 'app key not authorized',
      request: appKeyReq,
      appDoc: defaultApp,
      errorCode: 'permission-denied',
    },
  ];

  for (const c of appCases) {
    test(`app authorization / ${c.name}`, async () => {
      const authenticatedFunction = https.onCall(
        testFunction(c.allowedRoles ?? ['admin', 'member']),
      );
      const req = c.request ?? appReq;
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

  const defaultAppKey: AppKey = {
    value: 'app-key-value',
    permissions: {'app:publish': true} as Permissions,
    created: Timestamp.now(),
    lastUsed: null,
  };

  type AppKeyCase = {
    name: string;
    request?: TestRequest;
    uid?: string;
    appID?: string;
    appDoc?: App;
    appKeyDoc?: AppKey;
    permission?: RequiredPermission;
    errorCode?: FunctionsErrorCode;
    response?: TestResponse;
  };
  const appKeyCases: AppKeyCase[] = [
    {
      name: 'with required permission',
      appDoc: defaultApp,
      appKeyDoc: defaultAppKey,
      response: {
        appName: defaultApp.name,
        bar: appKeyReq.foo,
        success: true,
      },
    },
    {
      name: 'without required permission',
      appDoc: defaultApp,
      appKeyDoc: defaultAppKey,
      permission: 'rooms:create',
      errorCode: 'permission-denied',
    },
    {
      name: 'for wrong app',
      appDoc: defaultApp,
      appKeyDoc: defaultAppKey,
      appID: 'different-app',
      errorCode: 'permission-denied',
    },
    {
      name: 'does not match requester',
      uid: 'some-user-id',
      appDoc: defaultApp,
      appKeyDoc: defaultAppKey,
      errorCode: 'permission-denied',
    },
    {
      name: 'deleted app key',
      appDoc: defaultApp,
      errorCode: 'permission-denied',
    },
  ];

  for (const c of appKeyCases) {
    test(`app key authorization / ${c.name}`, async () => {
      const authenticatedFunction = https.onCall(
        testFunctionWithKeys(c.permission ?? 'app:publish'),
      );
      const req = c.request ?? appKeyReq;

      if (c.appDoc) {
        await firestore.doc(`apps/${APP_ID}`).set(c.appDoc);
      }
      if (c.appKeyDoc) {
        await firestore
          .doc(`apps/${APP_ID}/keys/${APP_KEY_NAME}`)
          .set(c.appKeyDoc);
      }

      let response: TestResponse | undefined;
      let error: HttpsError | undefined;
      try {
        response = await authenticatedFunction.run({
          auth: {uid: c.uid ?? req.requester.userID} as AuthData,
          data: {...req, appID: c.appID ?? APP_ID},
          rawRequest: null as unknown as Request,
        });
      } catch (e) {
        expect(e).toBeInstanceOf(HttpsError);
        error = e as HttpsError;
      }

      expect(error?.code).toBe(c.errorCode);
      expect(response).toEqual(c.response);

      if (c.response) {
        const key = await firestore
          .doc(`apps/${APP_ID}/keys/${APP_KEY_NAME}`)
          .get();
        expect(key?.data()?.lastUsed).toBeInstanceOf(Timestamp);
      }
    });
  }
});
