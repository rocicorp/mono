import {describe, expect, test} from 'vitest';
import type {BenchmarkConfig} from './config.ts';
import {sanitizeConfig} from './results.ts';

describe('sanitizeConfig', () => {
  test('redacts cloudzero.apiKey, adminPassword, and pg.url credentials', () => {
    const config = {
      profile: 'feed-append',
      model: 'hot',
      adminPassword: 'super-secret-password',
      pg: {
        url: 'postgresql://postgres:secretpassword@db.example.com:5432/testdb',
        start: false,
        stopAfterRun: true,
        readyTimeoutMs: 5000,
      },
      cloudzero: {
        apiKey: 'bearer-token-12345',
        metricsUrl: 'http://example.com',
        stackId: 'my-stack',
      },
    } as unknown as BenchmarkConfig;

    const sanitized = sanitizeConfig(config);
    expect(sanitized.adminPassword).toBe('<REDACTED>');
    expect(sanitized.cloudzero?.apiKey).toBe('<REDACTED>');
    expect(sanitized.cloudzero?.stackId).toBe('my-stack');
    expect(sanitized.pg.url).toBe(
      'postgresql://postgres:<REDACTED>@db.example.com:5432/testdb',
    );
  });

  test('handles undefined secrets gracefully', () => {
    const config = {
      profile: 'feed-append',
      model: 'hot',
      pg: {
        url: 'postgresql://localhost:5432/testdb',
        start: false,
        stopAfterRun: true,
        readyTimeoutMs: 5000,
      },
    } as unknown as BenchmarkConfig;

    const sanitized = sanitizeConfig(config);
    expect(sanitized.adminPassword).toBeUndefined();
    expect(sanitized.cloudzero).toBeUndefined();
    expect(sanitized.pg.url).toBe('postgresql://localhost:5432/testdb');
  });
});
