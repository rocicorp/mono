import {getMockRes} from '@jest-mock/express';
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import type {Auth} from 'firebase-admin/auth';
import {FieldValue, Timestamp, getFirestore} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import type {Request} from 'firebase-functions/v2/https';
import {
  Permissions,
  appKeyDataConverter,
  appKeyPath,
} from 'mirror-schema/src/app-key.js';
import {encryptUtf8} from 'mirror-schema/src/crypto.js';
import {appPath, defaultOptions} from 'mirror-schema/src/deployment.js';
import {
  DEFAULT_ENV,
  ENCRYPTION_KEY_SECRET_NAME,
  envDataConverter,
  envPath,
} from 'mirror-schema/src/env.js';
import {setApp} from 'mirror-schema/src/test-helpers.js';
import {
  APIErrorCode,
  APIResponse,
  makeAPIError,
  makeAPIResponse,
} from 'shared/src/api/responses.js';
import {FetchMocker} from 'shared/src/fetch-mocker.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {TestSecrets} from '../../secrets/test-utils.js';
import {dummyDeployment} from '../../test-helpers.js';
import {apps} from './apps.function.js';

describe('api-apps', () => {
  // Note: The Firestore emulator returns an explanation-free UNKNOWN error if there are
  // capital letters in the projectId, so don't capitalize anything there.
  initializeApp({projectId: 'api-apps-test'});
  const firestore = getFirestore();
  const APP_ID = 'api-app-id';
  const APP_KEY_NAME = 'my-app-key';
  const APP_KEY_VALUE = 'rHm_ELVQvsuj0GfZIF62A1BGUQE6NA8kZHwu8mF_UVo';

  function apiSuccessResponse<T>(result: T): Response {
    return {
      ok: true,
      status: 200,
      text: () =>
        Promise.resolve(
          JSON.stringify(makeAPIResponse(result as ReadonlyJSONValue)),
        ),
    } as unknown as Response;
  }

  function apiErrorResponse(code: number, message?: string): Response {
    return {
      ok: false,
      status: code,
      text: () =>
        Promise.resolve(
          JSON.stringify(
            makeAPIError({
              code: code as APIErrorCode,
              resource: 'request',
              message: message ?? 'no message',
            }),
          ),
        ),
    } as unknown as Response;
  }

  function testSecrets() {
    return new TestSecrets([
      ENCRYPTION_KEY_SECRET_NAME,
      '1',
      TestSecrets.TEST_KEY,
    ]);
  }

  beforeAll(async () => {
    const runningDeployment = dummyDeployment('1234');
    runningDeployment.spec.hostname = 'my-app-team.reflect-server.bonk';
    await Promise.all([
      setApp(firestore, APP_ID, {name: 'za app', runningDeployment}),
      firestore
        .doc(appKeyPath(APP_ID, APP_KEY_NAME))
        .withConverter(appKeyDataConverter)
        .create({
          value: APP_KEY_VALUE,
          permissions: {
            'rooms:read': true,
            'connections:invalidate': true,
          } as Permissions,
          created: Timestamp.now(),
          lastUsed: null,
        }),
      firestore
        .doc(envPath(APP_ID, DEFAULT_ENV))
        .withConverter(envDataConverter)
        .create({
          deploymentOptions: defaultOptions(),
          secrets: {
            ['REFLECT_API_KEY']: encryptUtf8(
              'the-reflect-api-key',
              Buffer.from(TestSecrets.TEST_KEY, 'base64url'),
              {version: '1'},
            ),
          },
        }),
    ]);
  });

  afterAll(async () => {
    // Clean up global emulator data.
    const batch = firestore.batch();
    batch.delete(firestore.doc(appPath(APP_ID)));
    batch.delete(firestore.doc(appKeyPath(APP_ID, APP_KEY_NAME)));
    batch.delete(firestore.doc(envPath(APP_ID, DEFAULT_ENV)));
    await batch.commit();
  });

  type Case = {
    name?: string;
    method: string;
    path: string;
    query?: string;
    token: string;
    workerUrl?: string;
    result: APIResponse<ReadonlyJSONValue>;
    pretest?: () => Promise<void>;
  };
  const cases: Case[] = [
    {
      method: 'GET',
      path: `/v1/apps/${APP_ID}/rooms/yo?dont=forgets&the=query`,
      token: APP_KEY_VALUE,
      workerUrl: `https://my-app-team.reflect-server.bonk/api/v1/rooms/yo?dont=forgets&the=query`,
      result: {
        result: {room: 'yo'},
        error: null,
      },
    },
    {
      method: 'POST',
      path: `/v1/apps/${APP_ID}/connections/all:invalidate`,
      token: APP_KEY_VALUE,
      workerUrl: `https://my-app-team.reflect-server.bonk/api/v1/connections/all:invalidate`,
      result: {
        result: {},
        error: null,
      },
    },
    {
      name: 'unsupported method',
      method: 'PUT',
      path: `/v1/apps/${APP_ID}/connections/all:invalidate`,
      token: APP_KEY_VALUE,
      result: {
        error: {
          code: 405,
          resource: 'request',
          message: 'Unsupported method "PUT"',
        },
        result: null,
      },
    },
    {
      name: 'invalid authorization header',
      method: 'GET',
      path: `/v1/apps/${APP_ID}/rooms/yo`,
      token: 'bad header with lots of spaces',
      result: {
        error: {
          code: 401 as APIErrorCode,
          resource: 'request',
          message: 'Invalid Authorization header',
        },
        result: null,
      },
    },
    {
      name: 'bad token',
      method: 'GET',
      path: `/v1/apps/${APP_ID}/rooms/yo`,
      token: 'bad-token',
      result: {
        error: {
          code: 403 as APIErrorCode,
          resource: 'request',
          message: 'Invalid key',
        },
        result: null,
      },
    },
    {
      name: 'missing permission',
      method: 'GET',
      path: `/v1/apps/${APP_ID}/connections/yo`,
      token: APP_KEY_VALUE,
      result: {
        error: {
          code: 403 as APIErrorCode,
          resource: 'request',
          message:
            'Key "my-app-key" has not been granted "connections:read" permission',
        },
        result: null,
      },
    },
    {
      name: 'key for wrong app',
      method: 'GET',
      path: `/v1/apps/wrong-app/rooms/yo`,
      token: APP_KEY_VALUE,
      result: {
        error: {
          code: 403 as APIErrorCode,
          resource: 'request',
          message: 'Key "my-app-key" is not authorized for app wrong-app',
        },
        result: null,
      },
    },
    {
      name: 'app not published yet',
      method: 'GET',
      path: `/v1/apps/${APP_ID}/rooms/yo`,
      token: APP_KEY_VALUE,
      result: {
        error: {
          code: 400,
          resource: 'request',
          message: 'App "za app" is not running',
        },
        result: null,
      },
      pretest: async () => {
        await firestore
          .doc(appPath(APP_ID))
          .update({runningDeployment: FieldValue.delete()});
      },
    },
  ];

  const auth = {
    verifyIdToken: jest
      .fn()
      .mockImplementation(() => Promise.resolve({uid: 'foo'})),
  };

  let fetchMocker: FetchMocker;
  beforeEach(() => {
    fetchMocker = new FetchMocker(apiSuccessResponse, apiErrorResponse)
      .result('GET', '/rooms/yo', {room: 'yo'})
      .result('POST', '/connections/all:invalidate', {});
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  cases.forEach(c =>
    test(c.name ?? `${c.method} ${c.path}`, async () => {
      if (c.pretest) {
        await c.pretest();
      }
      const url = `https://api.reflect-server.net${c.path}${c.query ?? ''}`;
      const request = {
        method: c.method,
        path: c.path,
        url,
        originalUrl: url,
        headers: {authorization: `Basic ${c.token}`},
        rawBody: Buffer.from('buffer body ^_^'),
      } as unknown as Request;
      const {res} = getMockRes();

      const appsFunction = https.onRequest(
        apps(firestore, auth as unknown as Auth, testSecrets()),
      );
      await appsFunction(request, res);

      if (c.workerUrl) {
        expect(res.send).toBeCalledWith(JSON.stringify(c.result));
        expect(fetchMocker.requests()).toEqual([[c.method, c.workerUrl]]);
        expect(fetchMocker.headers()).toEqual([
          {'x-reflect-api-key': 'the-reflect-api-key'},
        ]);
        expect(fetchMocker.bodys()).toEqual([Buffer.from('buffer body ^_^')]);
      } else {
        expect(res.json).toBeCalledWith(c.result);
      }
    }),
  );
});
