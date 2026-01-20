import {describe, expect, test, vi} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {isDevelopmentMode} from '../config/normalize.ts';
import {InspectorDelegate} from './inspector-delegate.ts';

// Mock the config module to control development mode
vi.mock('../config/normalize.ts', () => ({
  isDevelopmentMode: vi.fn(() => false),
}));

describe('InspectorDelegate', () => {
  test('addMetric accumulates metrics for global and per-query tracking', () => {
    const d = new InspectorDelegate(undefined);
    const queryID = 'test-query';
    const ast: AST = {table: 'users'};

    d.addQuery(queryID, ast);

    // Add multiple metrics
    d.addMetric('query-materialization-server', 5, queryID);
    d.addMetric('query-materialization-server', 15, queryID);
    d.addMetric('query-update-server', 3, queryID);

    const queryMetrics = d.getMetricsJSONForQuery(queryID);
    expect(queryMetrics).toEqual({
      'query-materialization-server': [1000, 5, 1, 15, 1], // Two centroids: 5 and 15
      'query-update-server': [1000, 3, 1], // One centroid: 3
    });

    const globalMetrics = d.getMetricsJSON();
    expect(globalMetrics).toEqual({
      'query-materialization-server': [1000, 5, 1, 15, 1],
      'query-update-server': [1000, 3, 1],
    });
  });

  test('getMetricsJSONForQuery returns null for non-existent query', () => {
    const d = new InspectorDelegate(undefined);
    expect(d.getMetricsJSONForQuery('non-existent')).toBe(null);
  });

  test('getASTForQuery returns undefined for non-existent query', () => {
    const d = new InspectorDelegate(undefined);
    expect(d.getASTForQuery('non-existent')).toBe(undefined);
  });

  test('removeQuery cleans up all associated data', () => {
    const d = new InspectorDelegate(undefined);
    const queryID = 'test-query';
    const ast: AST = {table: 'products'};

    d.addQuery(queryID, ast);
    d.addMetric('query-materialization-server', 10, queryID);

    // Verify data exists
    expect(d.getMetricsJSONForQuery(queryID)).not.toBe(null);
    expect(d.getASTForQuery(queryID)).toEqual(ast);

    // Remove query
    d.removeQuery(queryID);

    // Verify data is cleaned up
    expect(d.getMetricsJSONForQuery(queryID)).toBe(null);
    expect(d.getASTForQuery(queryID)).toBe(undefined);
  });

  test('removeQuery cleans up transformation hash when no queries remain', () => {
    const d = new InspectorDelegate(undefined);
    const q1 = 'query-1';
    const q2 = 'query-2';
    const ast: AST = {table: 'orders'};

    d.addQuery(q1, ast);
    d.addQuery(q2, ast);

    // Remove first query - hash should still exist
    d.removeQuery(q1);
    expect(d.getASTForQuery(q2)).toEqual(ast);

    // Remove second query - hash should be cleaned up
    d.removeQuery(q2);
    expect(d.getASTForQuery(q2)).toBe(undefined);
  });

  test('addQuery with same queryID updates existing mapping', () => {
    const d = new InspectorDelegate(undefined);
    const queryID = 'test-query';
    const ast1: AST = {table: 'table1'};
    const ast2: AST = {table: 'table2'};

    d.addQuery(queryID, ast1);
    expect(d.getASTForQuery(queryID)).toEqual(ast1);

    // Add same query with different AST - should update
    d.addQuery(queryID, ast2);
    expect(d.getASTForQuery(queryID)).toEqual(ast2);
  });

  test('metrics are isolated between different query ids', () => {
    const d = new InspectorDelegate(undefined);
    const q1 = 'query-1';
    const q2 = 'query-2';
    const ast: AST = {table: 'items'};

    d.addQuery(q1, ast);
    d.addQuery(q2, ast);

    d.addMetric('query-materialization-server', 10, q1);
    d.addMetric('query-materialization-server', 20, q2);

    const m1 = d.getMetricsJSONForQuery(q1);
    const m2 = d.getMetricsJSONForQuery(q2);

    expect(m1).toEqual({
      'query-materialization-server': [1000, 10, 1],
      'query-update-server': [1000], // Empty TDigest
    });

    expect(m2).toEqual({
      'query-materialization-server': [1000, 20, 1],
      'query-update-server': [1000], // Empty TDigest
    });
  });

  describe('Authentication', () => {
    test('isAuthenticated returns true in development mode', () => {
      vi.mocked(isDevelopmentMode).mockReturnValue(true);
      const d = new InspectorDelegate(undefined);

      expect(d.isAuthenticated('any-client')).toBe(true);
    });

    test('isAuthenticated returns false for unauthenticated client in production', () => {
      vi.mocked(isDevelopmentMode).mockReturnValue(false);
      const d = new InspectorDelegate(undefined);

      expect(d.isAuthenticated('client-1')).toBe(false);
    });

    test('setAuthenticated and isAuthenticated work together', () => {
      vi.mocked(isDevelopmentMode).mockReturnValue(false);
      const d = new InspectorDelegate(undefined);
      const clientID = 'client-123';

      expect(d.isAuthenticated(clientID)).toBe(false);

      d.setAuthenticated(clientID);
      expect(d.isAuthenticated(clientID)).toBe(true);
    });

    test('clearAuthenticated removes authentication', () => {
      vi.mocked(isDevelopmentMode).mockReturnValue(false);
      const d = new InspectorDelegate(undefined);
      const clientID = 'client-456';

      d.setAuthenticated(clientID);
      expect(d.isAuthenticated(clientID)).toBe(true);

      d.clearAuthenticated(clientID);
      expect(d.isAuthenticated(clientID)).toBe(false);
    });

    test('authentication state is shared across InspectorDelegate instances', () => {
      vi.mocked(isDevelopmentMode).mockReturnValue(false);
      const d1 = new InspectorDelegate(undefined);
      const d2 = new InspectorDelegate(undefined);
      const clientID = 'shared-client';

      d1.setAuthenticated(clientID);
      expect(d2.isAuthenticated(clientID)).toBe(true);

      d2.clearAuthenticated(clientID);
      expect(d1.isAuthenticated(clientID)).toBe(false);
    });
  });

  test('addMetric throws for invalid server metrics', () => {
    const d = new InspectorDelegate(undefined);

    expect(() => {
      // @ts-expect-error - Testing invalid metric
      d.addMetric('invalid-metric', 10, 'hash');
    }).toThrow('Invalid server metric: invalid-metric');
  });

  test('global metrics accumulate across all queries', () => {
    const d = new InspectorDelegate(undefined);
    const q1 = 'query-1';
    const q2 = 'query-2';
    const ast: AST = {table: 'global'};

    d.addQuery(q1, ast);
    d.addQuery(q2, ast);

    d.addMetric('query-materialization-server', 5, q1);
    d.addMetric('query-materialization-server', 15, q2);
    d.addMetric('query-update-server', 3, q1);
    d.addMetric('query-update-server', 7, q2);

    const globalMetrics = d.getMetricsJSON();
    expect(globalMetrics).toEqual({
      'query-materialization-server': [1000, 5, 1, 15, 1], // Two centroids: 5 and 15
      'query-update-server': [1000, 3, 1, 7, 1], // Two centroids: 3 and 7
    });
  });

  test('metrics are created lazily for queries', () => {
    const d = new InspectorDelegate(undefined);
    const queryID = 'test-query';
    const ast: AST = {table: 'lazy'};

    d.addQuery(queryID, ast);

    // No metrics added yet, should return null
    expect(d.getMetricsJSONForQuery(queryID)).toBe(null);

    // Add a metric, should create metrics object
    d.addMetric('query-materialization-server', 1, queryID);
    expect(d.getMetricsJSONForQuery(queryID)).not.toBe(null);
  });

  test('addMetric for non-existent query does not crash', () => {
    const d = new InspectorDelegate(undefined);

    // Should not throw even if no queries exist for this hash
    expect(() => {
      d.addMetric('query-materialization-server', 10, 'non-existent-query-id');
    }).not.toThrow();

    // Global metrics should still be updated
    const globalMetrics = d.getMetricsJSON();
    expect(globalMetrics).toEqual({
      'query-materialization-server': [1000, 10, 1],
      'query-update-server': [1000],
    });
  });

  test('removeQuery handles non-existent query gracefully', () => {
    const d = new InspectorDelegate(undefined);

    // Should not throw for non-existent query
    expect(() => {
      d.removeQuery('non-existent-query');
    }).not.toThrow();
  });

  test('empty metrics object has correct structure', () => {
    const d = new InspectorDelegate(undefined);

    const globalMetrics = d.getMetricsJSON();
    expect(globalMetrics).toEqual({
      'query-materialization-server': [1000], // Empty TDigest
      'query-update-server': [1000], // Empty TDigest
    });
  });
});
