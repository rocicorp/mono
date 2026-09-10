import {describe, expect, test} from 'vitest';
import type {BenchmarkConfig} from './config.ts';
import {lagSlope, sanitizeConfig} from './results.ts';

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

describe('lagSlope (OLS linear regression)', () => {
  test('returns 0 for fewer than 2 samples', () => {
    expect(lagSlope([])).toBe(0);
    expect(
      lagSlope([
        {
          elapsedMs: 0,
          committedSeq: 10,
          minObservedSeq: 10,
          seqLag: 0,
          connectedClients: 1,
        },
      ]),
    ).toBe(0);
  });

  test('computes exact slope for two samples', () => {
    expect(
      lagSlope([
        {
          elapsedMs: 0,
          committedSeq: 10,
          minObservedSeq: 10,
          seqLag: 0,
          connectedClients: 1,
        },
        {
          elapsedMs: 2000,
          committedSeq: 30,
          minObservedSeq: 10,
          seqLag: 20,
          connectedClients: 1,
        },
      ]),
    ).toBe(10); // 20 seq / 2 sec = 10 seq/s
  });

  test('computes zero slope for constant lag', () => {
    expect(
      lagSlope([
        {
          elapsedMs: 0,
          committedSeq: 10,
          minObservedSeq: 5,
          seqLag: 5,
          connectedClients: 1,
        },
        {
          elapsedMs: 2000,
          committedSeq: 25,
          minObservedSeq: 20,
          seqLag: 5,
          connectedClients: 1,
        },
        {
          elapsedMs: 4000,
          committedSeq: 45,
          minObservedSeq: 40,
          seqLag: 5,
          connectedClients: 1,
        },
      ]),
    ).toBe(0);
  });

  test('computes negative slope when lag is decreasing', () => {
    expect(
      lagSlope([
        {
          elapsedMs: 0,
          committedSeq: 30,
          minObservedSeq: 10,
          seqLag: 20,
          connectedClients: 1,
        },
        {
          elapsedMs: 2000,
          committedSeq: 40,
          minObservedSeq: 30,
          seqLag: 10,
          connectedClients: 1,
        },
        {
          elapsedMs: 4000,
          committedSeq: 50,
          minObservedSeq: 50,
          seqLag: 0,
          connectedClients: 1,
        },
      ]),
    ).toBe(-5); // -20 seq / 4 sec = -5 seq/s
  });
});
