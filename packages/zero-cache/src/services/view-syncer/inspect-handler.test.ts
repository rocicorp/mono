import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {TDigest} from '../../../../shared/src/tdigest.ts';
import type {QueryServerMetrics} from '../../../../zero-protocol/src/inspect-down.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import type {ClientHandler} from './client-handler.ts';
import type {ConnectionContext} from './connection-context-manager.ts';
import type {CVRStore} from './cvr-store.ts';
import type {CVRSnapshot} from './cvr.ts';
import {handleInspect, metricsForProtocol} from './inspect-handler.ts';

describe('handleInspect authentication', () => {
  const lc = createSilentLogContext();

  test('does not inspect an unauthenticated analyze-query AST', async () => {
    const sendInspectResponse = vi.fn();
    const ast = new Proxy(
      {},
      {
        get() {
          throw new Error('AST was inspected');
        },
        ownKeys() {
          throw new Error('AST was inspected');
        },
      },
    );

    await handleInspect(
      lc,
      {op: 'analyze-query', id: 'inspect-1', ast},
      {} as CVRSnapshot,
      {sendInspectResponse} as unknown as ClientHandler,
      {isAuthenticated: () => false} as unknown as InspectorDelegate,
      'client-group-1',
      {} as CVRStore,
      {} as NormalizedZeroConfig,
      {} as ConnectionContext,
    );

    expect(sendInspectResponse).toHaveBeenCalledWith(lc, {
      op: 'authenticated',
      id: 'inspect-1',
      value: false,
    });
  });

  test('validates an analyze-query AST after authentication', async () => {
    const sendInspectResponse = vi.fn();

    await handleInspect(
      lc,
      {op: 'analyze-query', id: 'inspect-1', ast: {}},
      {} as CVRSnapshot,
      {sendInspectResponse} as unknown as ClientHandler,
      {isAuthenticated: () => true} as unknown as InspectorDelegate,
      'client-group-1',
      {} as CVRStore,
      {} as NormalizedZeroConfig,
      {} as ConnectionContext,
    );

    expect(sendInspectResponse).toHaveBeenCalledWith(lc, {
      op: 'error',
      id: 'inspect-1',
      value: expect.any(String),
    });
  });
});

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
