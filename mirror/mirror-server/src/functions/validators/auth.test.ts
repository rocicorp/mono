import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
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
  ApiKey,
  Permissions,
  RequiredPermission,
} from 'mirror-schema/src/api-key.js';
import {apiKeyPath} from 'mirror-schema/src/api-key.js';
import {appPath, type App} from 'mirror-schema/src/app.js';
import type {Role} from 'mirror-schema/src/membership.js';
import {DEFAULT_PROVIDER_ID} from 'mirror-schema/src/provider.js';
import {setUser} from 'mirror-schema/src/test-helpers.js';
import type {User} from 'mirror-schema/src/user.js';
import {userPath} from 'mirror-schema/src/user.js';
import {FetchMocker} from 'shared/src/fetch-mocker.js';
import * as v from 'shared/src/valita.js';
import {mockFunctionParamsAndSecrets} from '../../test-helpers.js';
import {
  appAuthorization,
  appOrKeyAuthorization,
  teamAuthorization,
  teamOrKeyAuthorization,
  userAuthorization,
  userOrKeyAuthorization,
} from './auth.js';
import {validateSchema} from './schema.js';
import type {Callable} from './types.js';

const testRequestSchema = v.object({
  ...baseAppRequestFields,
  teamID: v.string(),
  foo: v.string(),
});

const testResponseSchema = v.object({
  ...baseResponseFields,
  authorizedFor: v.string(),
  bar: v.string(),
});

type TestRequest = v.Infer<typeof testRequestSchema>;
type TestResponse = v.Infer<typeof testResponseSchema>;

describe('auth-team-validators', () => {
  initializeApp({projectId: 'auth-validator-test'});
  const firestore = getFirestore();
  const USER_ID = 'auth-user-id';
  const APP_ID = 'auth-app-id';
  const TEAM_ID = 'auth-user-team';
  const API_KEY_NAME = 'auth-api-key';

  let fetchMocker: FetchMocker;

  mockFunctionParamsAndSecrets();

  beforeEach(() => {
    fetchMocker = new FetchMocker().result('POST', '/apiKeys-update', {});
  });

  afterEach(async () => {
    const batch = firestore.batch();
    batch.delete(firestore.doc(userPath(USER_ID)));
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(apiKeyPath(TEAM_ID, API_KEY_NAME)));
    await batch.commit();

    jest.clearAllMocks();
  });

  function testTeamFunction(
    allowedRoles: Role[] = ['admin', 'member'],
  ): Callable<TestRequest, TestResponse> {
    return validateSchema(testRequestSchema, testResponseSchema)
      .validate(userAuthorization())
      .validate(teamAuthorization(firestore, allowedRoles))
      .handle(
        // eslint-disable-next-line require-await
        async (testRequest, context) => ({
          authorizedFor: context.teamID,
          bar: testRequest.foo,
          success: true,
        }),
      );
  }

  function testTeamFunctionWithKeys(
    keyPermission: RequiredPermission,
  ): Callable<TestRequest, TestResponse> {
    return validateSchema(testRequestSchema, testResponseSchema)
      .validate(userOrKeyAuthorization())
      .validate(teamOrKeyAuthorization(firestore, keyPermission))
      .handle(
        // eslint-disable-next-line require-await
        async (testRequest, context) => ({
          authorizedFor: context.teamID,
          bar: testRequest.foo,
          success: true,
        }),
      );
  }

  function testAppFunction(
    allowedRoles: Role[] = ['admin', 'member'],
  ): Callable<TestRequest, TestResponse> {
    return validateSchema(testRequestSchema, testResponseSchema)
      .validate(userAuthorization())
      .validate(appAuthorization(firestore, allowedRoles))
      .handle(
        // eslint-disable-next-line require-await
        async (testRequest, context) => ({
          authorizedFor: context.app.name,
          bar: testRequest.foo,
          success: true,
        }),
      );
  }

  function testAppFunctionWithKeys(
    keyPermission: RequiredPermission,
  ): Callable<TestRequest, TestResponse> {
    return validateSchema(testRequestSchema, testResponseSchema)
      .validate(userOrKeyAuthorization())
      .validate(appOrKeyAuthorization(firestore, keyPermission))
      .handle(
        // eslint-disable-next-line require-await
        async (testRequest, context) => ({
          authorizedFor: context.app.name,
          bar: testRequest.foo,
          success: true,
        }),
      );
  }

  test('warmup request', async () => {
    const authenticatedFunction = https.onCall(testAppFunction());

    const resp = await authenticatedFunction.run({
      auth: {} as AuthData,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      data: {_warm_: true} as unknown as TestRequest,
      rawRequest: null as unknown as Request,
    });
    // eslint-disable-next-line @typescript-eslint/naming-convention
    expect(resp).toEqual({_warmed_: true});
  });

  const teamRequest = {
    requester: {
      userID: USER_ID,
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    foo: 'boo',
    teamID: TEAM_ID,
    appID: APP_ID,
  };

  type UserCase = {
    name: string;
    request: TestRequest;
    authData: AuthData;
    allowedRoles?: Role[];
    userRole?: Role;
    errorCode?: FunctionsErrorCode;
  };
  const userCases: UserCase[] = [
    {
      name: 'successful authentication',
      authData: {uid: USER_ID} as AuthData,
      request: teamRequest,
    },
    {
      name: 'insufficient team privileges',
      authData: {uid: USER_ID} as AuthData,
      request: teamRequest,
      allowedRoles: ['admin'],
      userRole: 'member',
      errorCode: 'permission-denied',
    },
    {
      name: 'missing authentication',
      authData: {} as AuthData,
      request: teamRequest,
      errorCode: 'unauthenticated',
    },
    {
      name: 'wrong authenticated user',
      authData: {uid: 'bar'} as AuthData,
      request: teamRequest,
      errorCode: 'permission-denied',
    },
    {
      name: 'user with super powers',
      authData: {
        uid: 'bar',
        token: {superUntil: Date.now() + 10000},
      } as unknown as AuthData,
      request: teamRequest,
    },
    {
      name: 'user with expired super powers',
      authData: {
        uid: 'bar',
        token: {superUntil: Date.now() - 10000},
      } as unknown as AuthData,
      request: teamRequest,
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
    test(`user/team authentication: ${c.name}`, async () => {
      await setUser(firestore, USER_ID, 'foo@bar.com', USER_ID, {
        [TEAM_ID]: c.userRole ?? 'admin',
      });

      const authenticatedFunction = https.onCall(
        testTeamFunction(c.allowedRoles ?? ['admin', 'member']),
      );

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
          authorizedFor: TEAM_ID,
          bar: 'boo',
          success: true,
        });
      }
    });
  }

  const defaultApiKey: ApiKey = {
    value: 'api-key-value',
    permissions: {'app:publish': true} as Permissions,
    created: Timestamp.now(),
    lastUsed: null,
    appIDs: [APP_ID],
  };

  const apiKeyTeamRequest = {
    requester: {
      userID: `teams/${TEAM_ID}/keys/${API_KEY_NAME}`,
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    foo: 'boo',
    teamID: TEAM_ID,
    appID: APP_ID,
  };

  type ApiKeyTeamCase = {
    name: string;
    uid?: string;
    teamID?: string;
    apiKeyDoc?: ApiKey;
    permission?: RequiredPermission;
    errorCode?: FunctionsErrorCode;
    response?: TestResponse;
  };

  const apiKeyTeamCases: ApiKeyTeamCase[] = [
    {
      name: 'with required permission',
      apiKeyDoc: defaultApiKey,
      response: {
        authorizedFor: TEAM_ID,
        bar: apiKeyTeamRequest.foo,
        success: true,
      },
    },
    {
      name: 'without required permission',
      apiKeyDoc: defaultApiKey,
      permission: 'rooms:create',
      errorCode: 'permission-denied',
    },
    {
      name: 'for wrong team',
      teamID: 'wrong-team',
      errorCode: 'permission-denied',
    },
    {
      name: 'does not match requester',
      uid: 'some-user-id',
      apiKeyDoc: defaultApiKey,
      errorCode: 'permission-denied',
    },
    {
      name: 'deleted team key',
      errorCode: 'permission-denied',
    },
  ];

  for (const c of apiKeyTeamCases) {
    test(`team key authorization / ${c.name}`, async () => {
      const authenticatedFunction = https.onCall(
        testTeamFunctionWithKeys(c.permission ?? 'app:publish'),
      );

      const req = apiKeyTeamRequest;
      if (c.apiKeyDoc) {
        await firestore
          .doc(`teams/${TEAM_ID}/keys/${API_KEY_NAME}`)
          .set(c.apiKeyDoc);
      }

      let response: TestResponse | undefined;
      let error: HttpsError | undefined;
      try {
        response = await authenticatedFunction.run({
          auth: {uid: c.uid ?? req.requester.userID} as AuthData,
          data: {...req, teamID: c.teamID ?? req.teamID},
          rawRequest: null as unknown as Request,
        });
      } catch (e) {
        expect(e).toBeInstanceOf(HttpsError);
        error = e as HttpsError;
      }

      expect(error?.code).toBe(c.errorCode);
      expect(response).toEqual(c.response);

      if (c.response) {
        expect(fetchMocker.requests()).toEqual([
          ['POST', 'http://127.0.0.1:5001/apiKeys-update'],
        ]);
        expect(fetchMocker.headers()).toEqual([
          {
            'Content-Type': 'application/json',
            'X-Mirror-Internal-Function': 'default-INTERNAL_FUNCTION_SECRET',
          },
        ]);
        const body = JSON.parse(String(fetchMocker.bodys()[0]));
        expect(body).toMatchObject({
          data: {
            teamID: TEAM_ID,
            keyName: API_KEY_NAME,
            lastUsed: expect.any(Number),
          },
        });
      }
    });
  }

  const defaultApp: App = {
    teamID: TEAM_ID,
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
    teamID: TEAM_ID,
    appID: APP_ID,
  };

  const apiKeyAppReq = {
    requester: {
      userID: `teams/${TEAM_ID}/keys/${API_KEY_NAME}`,
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    foo: 'boo',
    teamID: TEAM_ID,
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
        authorizedFor: defaultApp.name,
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
        authorizedFor: defaultApp.name,
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
      request: apiKeyAppReq,
      appDoc: defaultApp,
      errorCode: 'permission-denied',
    },
  ];

  for (const c of appCases) {
    test(`app authorization / ${c.name}`, async () => {
      const authenticatedFunction = https.onCall(
        testAppFunction(c.allowedRoles ?? ['admin', 'member']),
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

  type ApiKeyAppCase = {
    name: string;
    request?: TestRequest;
    uid?: string;
    appID?: string;
    appDoc?: App;
    apiKeyDoc?: ApiKey;
    permission?: RequiredPermission;
    errorCode?: FunctionsErrorCode;
    response?: TestResponse;
  };
  const apiKeyAppCases: ApiKeyAppCase[] = [
    {
      name: 'with required permission',
      appDoc: defaultApp,
      apiKeyDoc: defaultApiKey,
      response: {
        authorizedFor: defaultApp.name,
        bar: apiKeyAppReq.foo,
        success: true,
      },
    },
    {
      name: 'without required permission',
      appDoc: defaultApp,
      apiKeyDoc: defaultApiKey,
      permission: 'rooms:create',
      errorCode: 'permission-denied',
    },
    {
      name: 'for wrong app',
      appDoc: defaultApp,
      apiKeyDoc: defaultApiKey,
      appID: 'different-app',
      errorCode: 'permission-denied',
    },
    {
      name: 'does not match requester',
      uid: 'some-user-id',
      appDoc: defaultApp,
      apiKeyDoc: defaultApiKey,
      errorCode: 'permission-denied',
    },
    {
      name: 'deleted app key',
      appDoc: defaultApp,
      errorCode: 'permission-denied',
    },
  ];

  for (const c of apiKeyAppCases) {
    test(`app key authorization / ${c.name}`, async () => {
      const authenticatedFunction = https.onCall(
        testAppFunctionWithKeys(c.permission ?? 'app:publish'),
      );
      const req = c.request ?? apiKeyAppReq;

      if (c.appDoc) {
        await firestore.doc(`apps/${APP_ID}`).set(c.appDoc);
      }
      if (c.apiKeyDoc) {
        await firestore
          .doc(`teams/${TEAM_ID}/keys/${API_KEY_NAME}`)
          .set(c.apiKeyDoc);
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
        expect(fetchMocker.requests()).toEqual([
          ['POST', 'http://127.0.0.1:5001/apiKeys-update'],
        ]);
        expect(fetchMocker.headers()).toEqual([
          {
            'Content-Type': 'application/json',
            'X-Mirror-Internal-Function': 'default-INTERNAL_FUNCTION_SECRET',
          },
        ]);
        const body = JSON.parse(String(fetchMocker.bodys()[0]));
        expect(body).toMatchObject({
          data: {
            teamID: TEAM_ID,
            keyName: API_KEY_NAME,
            lastUsed: expect.any(Number),
          },
        });
      }
    });
  }
});
