import {describe, expect, test} from '@jest/globals';
import {initializeApp} from 'firebase-admin/app';
import {https} from 'firebase-functions/v2';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import type {ErrorReportingRequest} from 'mirror-protocol/src/error.js';
import {report} from './report.function.js';

describe('error-report function', () => {
  initializeApp({projectId: 'error-report-function-test'});

  const errorReportingFunction = https.onCall(report());

  const request: ErrorReportingRequest = {
    requester: {
      userID: 'foo',
      userAgent: {type: 'reflect-cli', version: '0.0.1'},
    },
    error: {
      desc: 'error-reporting-test',
      name: 'Error',
      message: 'error-reporting-test',
      stack: 'error-reporting-test',
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

  test('request push an error', async () => {
    try {
      const resp = await errorReportingFunction.run({
        data: request,
        rawRequest: null as unknown as Request,
      });
      console.log(resp);
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('unknown');
      expect((e as HttpsError).message).toBe(
        'action: error-reporting-test, description: error-reporting-test',
      );
    }
  });

  test('request push a warning', async () => {
    try {
      const resp = await errorReportingFunction.run({
        data: {
          ...request,
          severity: 'WARNING',
        },
        rawRequest: null as unknown as Request,
      });
      console.log(resp);
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('cancelled');
      expect((e as HttpsError).message).toBe(
        'action: error-reporting-test, description: error-reporting-test',
      );
    }
  });

  test('request from roci team', async () => {
    try {
      const resp = await errorReportingFunction.run({
        data: {
          ...request,
          requester: {
            userID: 'hu0ggohMptVpC4GRn6GhfN9dhcO2',
            userAgent: {type: 'reflect-cli', version: '0.0.1'},
          },
        },
        rawRequest: null as unknown as Request,
      });
      console.log(resp);
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('aborted');
      expect((e as HttpsError).message).toBe(
        'action: error-reporting-test, description: error-reporting-test',
      );
    }
  });

  test('FirebaseError', async () => {
    try {
      const resp = await errorReportingFunction.run({
        data: {
          ...request,
          error: {
            desc: 'error-reporting-test',
            name: 'FirebaseError',
            message: 'error-reporting-test',
            stack: 'error-reporting-test',
          },
        },
        rawRequest: null as unknown as Request,
      });
      console.log(resp);
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('already-exists');
      expect((e as HttpsError).message).toBe(
        'action: error-reporting-test, description: error-reporting-test',
      );
    }
  });
});
