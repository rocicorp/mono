import {describe, expect, test, vi} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {isDevelopmentMode} from '../config/normalize.ts';
import {InspectorDelegate} from './inspector-delegate.ts';

// Mock the config module to control development mode
vi.mock('../config/normalize.ts', () => ({
  isDevelopmentMode: vi.fn(() => false),
}));

describe('InspectorDelegate', () => {
  test('routes one query meta data to all queries sharing a transformationHash', () => {
    const d = new InspectorDelegate();

    const hash = 'same-xform-hash';
    const q1 = 'query-A';
    const q2 = 'query-B';
    const ast: AST = {table: 'issues'};

    d.addQuery(hash, q1, ast);
    d.addQuery(hash, q2, ast);

    // Emit metrics for that transformation
    d.addMetric('query-update-server', 10, hash);
    d.addMetric('query-materialization-server', 7, hash);

    const m1 = d.getMetricsJSONForQuery(q1);
    const m2 = d.getMetricsJSONForQuery(q2);

    expect(m1).toEqual({
      'query-materialization-server': [1000, 7, 1],
      'query-update-server': [1000, 10, 1],
    });

    expect(m1).toEqual(m2);

    expect(d.getASTForQuery(q1)).toEqual(ast);
    expect(d.getASTForQuery(q2)).toEqual(ast);
  });

  test('addMetric accumulates metrics for global and per-query tracking', () => {
    const d = new InspectorDelegate();
    const hash = 'test-hash';
    const queryID = 'test-query';
    const ast: AST = {table: 'users'};

    d.addQuery(hash, queryID, ast);

    // Add multiple metrics
    d.addMetric('query-materialization-server', 5, hash);
    d.addMetric('query-materialization-server', 15, hash);
    d.addMetric('query-update-server', 3, hash);

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
    const d = new InspectorDelegate();
    expect(d.getMetricsJSONForQuery('non-existent')).toBe(null);
  });

  test('getASTForQuery returns undefined for non-existent query', () => {
    const d = new InspectorDelegate();
    expect(d.getASTForQuery('non-existent')).toBe(undefined);
  });

  test('removeQuery cleans up all associated data', () => {
    const d = new InspectorDelegate();
    const hash = 'test-hash';
    const queryID = 'test-query';
    const ast: AST = {table: 'products'};

    d.addQuery(hash, queryID, ast);
    d.addMetric('query-materialization-server', 10, hash);

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
    const d = new InspectorDelegate();
    const hash = 'shared-hash';
    const q1 = 'query-1';
    const q2 = 'query-2';
    const ast: AST = {table: 'orders'};

    d.addQuery(hash, q1, ast);
    d.addQuery(hash, q2, ast);

    // Remove first query - hash should still exist
    d.removeQuery(q1);
    expect(d.getASTForQuery(q2)).toEqual(ast);

    // Remove second query - hash should be cleaned up
    d.removeQuery(q2);
    expect(d.getASTForQuery(q2)).toBe(undefined);
  });

  test('addQuery with same hash and queryID updates existing mapping', () => {
    const d = new InspectorDelegate();
    const hash = 'test-hash';
    const queryID = 'test-query';
    const ast1: AST = {table: 'table1'};
    const ast2: AST = {table: 'table2'};

    d.addQuery(hash, queryID, ast1);
    expect(d.getASTForQuery(queryID)).toEqual(ast1);

    // Add same query with different AST - should update
    d.addQuery(hash, queryID, ast2);
    expect(d.getASTForQuery(queryID)).toEqual(ast2);
  });

  test('metrics are isolated between different transformation hashes', () => {
    const d = new InspectorDelegate();
    const hash1 = 'hash-1';
    const hash2 = 'hash-2';
    const q1 = 'query-1';
    const q2 = 'query-2';
    const ast: AST = {table: 'items'};

    d.addQuery(hash1, q1, ast);
    d.addQuery(hash2, q2, ast);

    d.addMetric('query-materialization-server', 10, hash1);
    d.addMetric('query-materialization-server', 20, hash2);

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
      const d = new InspectorDelegate();

      expect(d.isAuthenticated('any-client')).toBe(true);
    });

    test('isAuthenticated returns false for unauthenticated client in production', () => {
      vi.mocked(isDevelopmentMode).mockReturnValue(false);
      const d = new InspectorDelegate();

      expect(d.isAuthenticated('client-1')).toBe(false);
    });

    test('setAuthenticated and isAuthenticated work together', () => {
      vi.mocked(isDevelopmentMode).mockReturnValue(false);
      const d = new InspectorDelegate();
      const clientID = 'client-123';

      expect(d.isAuthenticated(clientID)).toBe(false);

      d.setAuthenticated(clientID);
      expect(d.isAuthenticated(clientID)).toBe(true);
    });

    test('clearAuthenticated removes authentication', () => {
      vi.mocked(isDevelopmentMode).mockReturnValue(false);
      const d = new InspectorDelegate();
      const clientID = 'client-456';

      d.setAuthenticated(clientID);
      expect(d.isAuthenticated(clientID)).toBe(true);

      d.clearAuthenticated(clientID);
      expect(d.isAuthenticated(clientID)).toBe(false);
    });

    test('authentication state is shared across InspectorDelegate instances', () => {
      vi.mocked(isDevelopmentMode).mockReturnValue(false);
      const d1 = new InspectorDelegate();
      const d2 = new InspectorDelegate();
      const clientID = 'shared-client';

      d1.setAuthenticated(clientID);
      expect(d2.isAuthenticated(clientID)).toBe(true);

      d2.clearAuthenticated(clientID);
      expect(d1.isAuthenticated(clientID)).toBe(false);
    });
  });

  test('addMetric throws for invalid server metrics', () => {
    const d = new InspectorDelegate();

    expect(() => {
      // @ts-expect-error - Testing invalid metric
      d.addMetric('invalid-metric', 10, 'hash');
    }).toThrow('Invalid server metric: invalid-metric');
  });

  test('global metrics accumulate across all queries', () => {
    const d = new InspectorDelegate();
    const hash1 = 'hash-1';
    const hash2 = 'hash-2';
    const ast: AST = {table: 'global'};

    d.addQuery(hash1, 'q1', ast);
    d.addQuery(hash2, 'q2', ast);

    d.addMetric('query-materialization-server', 5, hash1);
    d.addMetric('query-materialization-server', 15, hash2);
    d.addMetric('query-update-server', 3, hash1);
    d.addMetric('query-update-server', 7, hash2);

    const globalMetrics = d.getMetricsJSON();
    expect(globalMetrics).toEqual({
      'query-materialization-server': [1000, 5, 1, 15, 1], // Two centroids: 5 and 15
      'query-update-server': [1000, 3, 1, 7, 1], // Two centroids: 3 and 7
    });
  });

  test('metrics are created lazily for queries', () => {
    const d = new InspectorDelegate();
    const hash = 'test-hash';
    const queryID = 'test-query';
    const ast: AST = {table: 'lazy'};

    d.addQuery(hash, queryID, ast);

    // No metrics added yet, should return null
    expect(d.getMetricsJSONForQuery(queryID)).toBe(null);

    // Add a metric, should create metrics object
    d.addMetric('query-materialization-server', 1, hash);
    expect(d.getMetricsJSONForQuery(queryID)).not.toBe(null);
  });

  test('addMetric for non-existent transformation hash does not crash', () => {
    const d = new InspectorDelegate();

    // Should not throw even if no queries exist for this hash
    expect(() => {
      d.addMetric('query-materialization-server', 10, 'non-existent-hash');
    }).not.toThrow();

    // Global metrics should still be updated
    const globalMetrics = d.getMetricsJSON();
    expect(globalMetrics).toEqual({
      'query-materialization-server': [1000, 10, 1],
      'query-update-server': [1000],
    });
  });

  test('multiple queries with same ID but different hashes', () => {
    const d = new InspectorDelegate();
    const queryID = 'same-query-id';
    const hash1 = 'hash-1';
    const hash2 = 'hash-2';
    const ast1: AST = {table: 'table1'};
    const ast2: AST = {table: 'table2'};

    // Add same query ID with different hashes
    d.addQuery(hash1, queryID, ast1);
    d.addQuery(hash2, queryID, ast2);

    // The query should now be associated with hash2 (latest)
    expect(d.getASTForQuery(queryID)).toEqual(ast2);

    d.addMetric('query-materialization-server', 5, hash1);
    d.addMetric('query-materialization-server', 10, hash2);

    // Query gets metrics from both hashes since it's in both sets
    const metrics = d.getMetricsJSONForQuery(queryID);
    expect(metrics).toEqual({
      'query-materialization-server': [1000, 5, 1, 10, 1], // Both metrics
      'query-update-server': [1000],
    });
  });

  test('removeQuery handles non-existent query gracefully', () => {
    const d = new InspectorDelegate();

    // Should not throw for non-existent query
    expect(() => {
      d.removeQuery('non-existent-query');
    }).not.toThrow();
  });

  test('empty metrics object has correct structure', () => {
    const d = new InspectorDelegate();

    const globalMetrics = d.getMetricsJSON();
    expect(globalMetrics).toEqual({
      'query-materialization-server': [1000], // Empty TDigest
      'query-update-server': [1000], // Empty TDigest
    });
  });
});
