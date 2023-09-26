import {declaredParams} from 'firebase-functions/params';
import assert from 'node:assert';

assert(process.env.NODE_ENV === 'test', 'Only import this file in tests');

export function mockFunctionParamsAndSecrets() {
  for (const p of declaredParams) {
    switch (p.name) {
      case 'DATADOG_LOGS_API_KEY':
      case 'DATADOG_METRICS_API_KEY':
        process.env[p.name] = `default-${p.name}`;
        break;
    }
  }
}
