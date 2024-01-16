import {getMockReq as jestGetMockReq} from '@jest-mock/express';
import type {MockRequest} from '@jest-mock/express/dist/src/request/index.js';
import {jest} from '@jest/globals';
import {Timestamp} from 'firebase-admin/firestore';
import {declaredParams} from 'firebase-functions/params';
import type {Request} from 'firebase-functions/v2/https';
import type {Deployment} from 'mirror-schema/src/deployment.js';
import assert from 'node:assert';
import {INTERNAL_FUNCTION_SECRET_NAME} from './functions/internal/auth.js';

assert(process.env.NODE_ENV === 'test', 'Only import this file in tests');

export function mockFunctionParamsAndSecrets() {
  for (const p of declaredParams) {
    switch (p.name) {
      case 'DATADOG_LOGS_API_KEY':
      case 'DATADOG_METRICS_API_KEY':
      case INTERNAL_FUNCTION_SECRET_NAME:
        process.env[p.name] = `default-${p.name}`;
        break;
    }
  }
}

export function dummyDeployment(deploymentID: string): Deployment {
  return {
    deploymentID,
    requesterID: 'SYSTEM',
    type: 'SERVER_UPDATE',
    status: 'RUNNING',
    requestTime: Timestamp.now(),
    spec: {
      appModules: [],
      serverVersion: '0.36.0',
      serverVersionRange: '^0.36.0',
      hostname: 'foo.bar',
      envUpdateTime: Timestamp.now(),
    },
  };
}

export function getMockReq(values: MockRequest): Request {
  // Replicate express's get() and header() behavior.
  const headers = Object.fromEntries(
    Object.entries(values.headers ?? {}).map(([key, value]) => [
      key.toLowerCase(),
      value,
    ]),
  );
  const header = jest
    .fn()
    .mockImplementation(name => headers[String(name).toLowerCase()]);
  return jestGetMockReq<Request>({
    ...values,
    get: values.get ?? header,
    header: values.header ?? header,
  });
}
