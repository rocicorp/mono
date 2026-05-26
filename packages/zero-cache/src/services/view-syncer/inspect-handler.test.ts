import {TDigest} from 'shared/src/tdigest.ts';
import {describe, expect, test} from 'vitest';
import type {QueryServerMetrics} from 'zero-protocol/src/inspect-down.ts';
import {metricsForProtocol} from './inspect-handler.ts';

describe('metricsForProtocol', () => {
  test('returns null unchanged', () => {
    expect(metricsForProtocol(null, 51)).toBeNull();
    expect(metricsForProtocol(null, 50)).toBeNull();
    expect(metricsForProtocol(null, 1)).toBeNull();
  });

  test('protocol >= 51: returns metrics as-is', () => {
    const updateDigest = new TDigest();
    updateDigest.add(10);
    updateDigest.add(20);
    const metrics = {
      'query-hydration-server-ms': 42,
      'query-update-server': updateDigest.toJSON(),
    };
    expect(metricsForProtocol(metrics, 51)).toBe(metrics);
    expect(metricsForProtocol(metrics, 52)).toBe(metrics);
    expect(metricsForProtocol(metrics, 100)).toBe(metrics);
  });

  test('protocol >= 51: returns metrics with no fields as-is', () => {
    const metrics = {} as unknown as QueryServerMetrics;
    expect(metricsForProtocol(metrics, 51)).toBe(metrics);
  });

  test('protocol < 51: wraps hydration ms into legacy TDigest field', () => {
    const updateDigest = new TDigest();
    updateDigest.add(5);
    const updateJSON = updateDigest.toJSON();

    const metrics = {
      'query-hydration-server-ms': 100,
      'query-update-server': updateJSON,
    };

    const result = metricsForProtocol(metrics, 50);
    expect(result).not.toBeNull();

    // Should have the legacy field, not the new one
    expect(result).not.toHaveProperty('query-hydration-server-ms');
    expect(result).toHaveProperty('query-materialization-server');
    expect(result).toHaveProperty('query-update-server', updateJSON);

    // The legacy TDigest should contain the single hydration value
    const materializationDigest = TDigest.fromJSON(
      (result as Record<string, unknown>)[
        'query-materialization-server'
      ] as ReturnType<TDigest['toJSON']>,
    );
    expect(materializationDigest.count()).toBe(1);
    expect(materializationDigest.quantile(0.5)).toBe(100);
  });

  test('protocol < 51: handles missing hydration-ms (wraps empty TDigest)', () => {
    const updateDigest = new TDigest();
    updateDigest.add(7);
    const updateJSON = updateDigest.toJSON();

    const metrics = {
      'query-update-server': updateJSON,
    };

    const result = metricsForProtocol(metrics, 50);
    expect(result).not.toBeNull();
    expect(result).toHaveProperty('query-materialization-server');
    expect(result).toHaveProperty('query-update-server', updateJSON);

    // The legacy TDigest should be empty (no value was added)
    const materializationDigest = TDigest.fromJSON(
      (result as Record<string, unknown>)[
        'query-materialization-server'
      ] as ReturnType<TDigest['toJSON']>,
    );
    expect(materializationDigest.count()).toBe(0);
  });

  test('protocol < 51: handles undefined update-server', () => {
    const metrics = {
      'query-hydration-server-ms': 25,
    } as unknown as QueryServerMetrics;

    const result = metricsForProtocol(metrics, 1);
    expect(result).not.toBeNull();
    expect(result).toHaveProperty('query-materialization-server');
    expect(
      (result as Record<string, unknown>)['query-update-server'],
    ).toBeUndefined();

    const materializationDigest = TDigest.fromJSON(
      (result as Record<string, unknown>)[
        'query-materialization-server'
      ] as ReturnType<TDigest['toJSON']>,
    );
    expect(materializationDigest.count()).toBe(1);
    expect(materializationDigest.quantile(0.5)).toBe(25);
  });
});
