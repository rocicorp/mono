import {describe, expect, test} from '@jest/globals';

import {https} from 'firebase-functions/v2';
import {
  FunctionsErrorCode,
  HttpsError,
  Request,
} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';
import {validateSchema} from './schema.js';
import {
  baseRequestFields,
  baseResponseFields,
} from 'mirror-protocol/src/base.js';
import * as v from 'shared/src/valita.js';
import type {Callable} from './types.js';
import {userAgentVersion, type DistTags} from './version.js';
import {SemVer} from 'semver';

const testRequestSchema = v.object({
  ...baseRequestFields,
});

const testResponseSchema = v.object({
  ...baseResponseFields,
  distTags: v.array(v.string()),
});

type TestRequest = v.Infer<typeof testRequestSchema>;
type TestResponse = v.Infer<typeof testResponseSchema>;

function testFunction(
  testDistTags: DistTags,
): Callable<TestRequest, TestResponse> {
  return validateSchema(testRequestSchema, testResponseSchema)
    .validate(userAgentVersion(testDistTags))
    .handle(
      // eslint-disable-next-line require-await
      async (_, context) => ({
        distTags: Object.keys(context.distTags),
        success: true,
      }),
    );
}

describe('user agent version check', () => {
  type Case = {
    name: string;
    agent: string;
    version: string;
    distTags: {[tag: string]: string};
    errorCode?: FunctionsErrorCode;
  };
  const cases: Case[] = [
    {
      name: 'no @sup tag',
      agent: 'reflect-cli',
      version: '0.35.1',
      distTags: {},
    },
    {
      name: 'supported agent',
      agent: 'reflect-cli',
      version: '0.35.1',
      distTags: {sup: '0.35.0'},
    },
    {
      name: 'unsupported agent',
      agent: 'reflect-cli',
      version: '0.35.1',
      distTags: {sup: '0.36.0'},
      errorCode: 'unavailable',
    },
    {
      name: 'unregulated agent',
      agent: 'web',
      version: '0.35.1',
      distTags: {sup: '0.36.0'},
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const distTags = Object.fromEntries(
        Object.entries(c.distTags).map(([tag, ver]) => [tag, new SemVer(ver)]),
      );
      const authenticatedFunction = https.onCall(testFunction(distTags));

      let error: HttpsError | undefined;
      let resp: TestResponse | undefined;
      try {
        resp = await authenticatedFunction.run({
          auth: null as unknown as AuthData,
          data: {
            requester: {
              userID: 'foo',
              userAgent: {
                type: c.agent,
                version: c.version,
              },
            },
          },
          rawRequest: null as unknown as Request,
        });
      } catch (e) {
        expect(e).toBeInstanceOf(HttpsError);
        error = e as HttpsError;
      }

      expect(error?.code).toBe(c.errorCode);
      if (!c.errorCode) {
        expect(resp).toEqual({
          distTags: Object.keys(c.distTags),
          success: true,
        });
      }
    });
  }
});
