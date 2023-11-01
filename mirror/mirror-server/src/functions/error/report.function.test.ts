import {describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import {https} from 'firebase-functions/v2';
import {
  FunctionsErrorCode,
  HttpsError,
  type Request,
} from 'firebase-functions/v2/https';
import type {ErrorReportingRequest} from 'mirror-protocol/src/error.js';
import {report} from './report.function.js';

describe('error-report function', () => {
  initializeApp({projectId: 'error-report-function-test'});

  const errorReportingFunction = https.onCall(report());

  const stack = 'This is the original\n    stack\n    trace';

  const request: ErrorReportingRequest = {
    requester: {
      userID: 'foo',
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    error: {
      desc: 'error-reporting-test',
      name: 'Error',
      message: 'error-reporting-test',
      stack,
    },
    severity: 'ERROR',
    agentContext: {
      'up.reflect_os_architecture': 'x86_64',
      'up.reflect_os_name': 'Mac OS X',
      'up.reflect_os_version': '10.15.7',
      'up.reflect_version': '0.0.1',
    },
    action: 'error-reporting-test',
  };

  type Case = {
    name: string;
    request: Partial<ErrorReportingRequest>;
    code: FunctionsErrorCode;
  };
  const cases: Case[] = [
    {
      name: 'request push an error',
      request: {},
      code: 'unknown',
    },
    {
      name: 'request push a warning',
      request: {severity: 'WARNING'},
      code: 'cancelled',
    },
    {
      name: 'request from roci team',
      request: {
        requester: {
          userID: 'hu0ggohMptVpC4GRn6GhfN9dhcO2',
          userAgent: {type: 'reflect-cli', version: '0.0.1'},
        },
      },
      code: 'aborted',
    },
    {
      name: 'FirebaseError',
      request: {
        error: {
          desc: 'error-reporting-test',
          name: 'FirebaseError',
          message: 'error-reporting-test',
          stack,
        },
      },
      code: 'already-exists',
    },
    {
      name: 'dev watch error',
      request: {
        action: 'cmd_dev',
        error: {
          desc: "Error: ENOENT: no such file or directory, open 'reflect/index.ts'",
          stack,
        },
      },
      code: 'cancelled',
    },
    {
      name: 'dev Miniflare ERR_RUNTIME_FAILURE',
      request: {
        action: 'cmd_dev',
        error: {
          desc:
            'MiniflareCoreError [ERR_RUNTIME_FAILURE]: The Workers runtime failed to start. ' +
            'There is likely additional logging output above.',
          stack,
        },
      },
      code: 'cancelled',
    },
    {
      name: 'init npm error',
      request: {
        action: 'cmd_init',
        error: {
          desc: `Error: Command failed: npm add '@rocicorp/reflect@^0.36.202310172246+95297b'`,
          stack,
        },
      },
      code: 'cancelled',
    },
    {
      name: 'create npm error',
      request: {
        action: 'cmd_create',
        error: {
          desc: `Error: Command failed: npm init '@rocicorp/reflect@^0.36.202310172246+95297b'`,
          stack,
        },
      },
      code: 'cancelled',
    },
    {
      name: 'publish compilation error',
      request: {
        action: 'cmd_publish',
        error: {
          desc:
            `Error: Build failed with 1 error:\n` +
            `dev/testReflectWorker.ts:5:2: ERROR: No matching export in "../../packages/dir/src/model/mutators/testSGWorkerMutators.ts" for import "setEnv"`,
          stack,
        },
      },
      code: 'cancelled',
    },
  ];
  for (const c of cases) {
    test(c.name, async () => {
      const data = {
        ...request,
        ...c.request,
      };
      try {
        const resp = await errorReportingFunction.run({
          data,
          rawRequest: null as unknown as Request,
        });
        console.log(resp);
      } catch (e) {
        expect(e).toBeInstanceOf(HttpsError);
        expect((e as HttpsError).code).toBe(c.code);
        expect((e as HttpsError).stack).toBe(
          `action: ${data.action}, description: ${stack}`,
        );
      }
    });
  }
});
