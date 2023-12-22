import {Timestamp} from 'firebase-admin/firestore';
import {declaredParams} from 'firebase-functions/params';
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
