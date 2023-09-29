import {describe, expect, test} from '@jest/globals';
import {https} from 'firebase-functions/v2';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import type {ErrorReportingRequest} from 'mirror-protocol/src/app.js';
import {initializeApp} from 'firebase-admin/app';
import {errorReporting} from './error-reporting.function.js';

describe('error-report function', () => {
  initializeApp({projectId: 'error-report-function-test'});

  const errorReportingFunction = https.onCall(errorReporting());

  const request: ErrorReportingRequest = {
    errorMessage: 'error-reporting-test',
  } as const;

  test('requests delete deployment', async () => {
    try {
      const resp = await errorReportingFunction.run({
        data: request,
        rawRequest: null as unknown as Request,
      });
      console.log(resp);
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      expect((e as HttpsError).code).toBe('unknown');
      expect((e as HttpsError).message).toBe('error-reporting-test');
    }
  });
});
