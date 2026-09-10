import {describe, expect, test} from 'vitest';
import type {BenchmarkConfig} from './config.ts';
import {sanitizeConfig} from './results.ts';

describe('sanitizeConfig', () => {
  test('redacts cloudzero.apiKey and adminPassword', () => {
    const config = {
      profile: 'feed-append',
      model: 'hot',
      adminPassword: 'super-secret-password',
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
  });

  test('handles undefined secrets gracefully', () => {
    const config = {
      profile: 'feed-append',
      model: 'hot',
    } as unknown as BenchmarkConfig;

    const sanitized = sanitizeConfig(config);
    expect(sanitized.adminPassword).toBeUndefined();
    expect(sanitized.cloudzero).toBeUndefined();
  });
});
