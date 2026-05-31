import {describe, expect, test} from 'vitest';
import * as valita from '../../shared/src/valita.ts';
import {
  analyzeQueryResultSchema,
  rowCountsByQuerySchema,
  rowCountsBySourceSchema,
  rowsByQuerySchema,
  rowsBySourceSchema,
  type AnalyzeQueryResult,
  type RowCountsByQuery,
  type RowCountsBySource,
  type RowsByQuery,
  type RowsBySource,
} from './analyze-query-result.ts';

describe('analyze-query-result schemas', () => {
  describe('rowCountsByQuerySchema', () => {
    test('validates valid row counts by query', () => {
      const valid: RowCountsByQuery = {
        'SELECT * FROM users': 5,
        'SELECT * FROM posts WHERE user_id = ?': 12,
      };

      expect(() => valita.parse(valid, rowCountsByQuerySchema)).not.toThrow();
      expect(valita.parse(valid, rowCountsByQuerySchema)).toEqual(valid);
    });

    test('rejects invalid row counts', () => {
      const invalid = {
        'SELECT * FROM users': 'not-a-number',
      };

      expect(() => valita.parse(invalid, rowCountsByQuerySchema)).toThrow();
    });

    test('handles empty object', () => {
      const empty: RowCountsByQuery = {};
      expect(() => valita.parse(empty, rowCountsByQuerySchema)).not.toThrow();
    });
  });

  describe('rowCountsBySourceSchema', () => {
    test('validates valid row counts by source', () => {
      const valid: RowCountsBySource = {
        users: {
          'SELECT * FROM users': 5,
          'SELECT * FROM users WHERE id = ?': 1,
        },
        posts: {
          'SELECT * FROM posts': 100,
        },
      };

      expect(() => valita.parse(valid, rowCountsBySourceSchema)).not.toThrow();
      expect(valita.parse(valid, rowCountsBySourceSchema)).toEqual(valid);
    });

    test('handles nested empty objects', () => {
      const valid: RowCountsBySource = {
        users: {},
        posts: {},
      };

      expect(() => valita.parse(valid, rowCountsBySourceSchema)).not.toThrow();
    });
  });

  describe('rowsByQuerySchema', () => {
    test('validates valid rows by query', () => {
      const valid: RowsByQuery = {
        'SELECT * FROM users': [
          {id: 1, name: 'Alice'},
          {id: 2, name: 'Bob'},
        ],
        'SELECT * FROM posts': [{id: 1, title: 'Post 1', ['user_id']: 1}],
      };

      expect(() => valita.parse(valid, rowsByQuerySchema)).not.toThrow();
      expect(valita.parse(valid, rowsByQuerySchema)).toEqual(valid);
    });

    test('handles empty arrays', () => {
      const valid: RowsByQuery = {
        'SELECT * FROM empty_table': [],
      };

      expect(() => valita.parse(valid, rowsByQuerySchema)).not.toThrow();
    });
  });

  describe('rowsBySourceSchema', () => {
    test('validates valid rows by source', () => {
      const valid: RowsBySource = {
        users: {
          'SELECT * FROM users': [
            {id: 1, name: 'Alice'},
            {id: 2, name: 'Bob'},
          ],
        },
        posts: {
          'SELECT * FROM posts': [{id: 1, title: 'Post 1', ['user_id']: 1}],
        },
      };

      expect(() => valita.parse(valid, rowsBySourceSchema)).not.toThrow();
      expect(valita.parse(valid, rowsBySourceSchema)).toEqual(valid);
    });
  });

  describe('analyzeQueryResultSchema', () => {
    test('validates minimal valid result', () => {
      const minimal: AnalyzeQueryResult = {
        warnings: [],
        syncedRowCount: 0,
        start: 1000,
        end: 1100,
      };

      expect(() =>
        valita.parse(minimal, analyzeQueryResultSchema),
      ).not.toThrow();
      expect(valita.parse(minimal, analyzeQueryResultSchema)).toEqual(minimal);
    });

    test('validates complete result with all optional fields', () => {
      const complete: AnalyzeQueryResult = {
        warnings: ['No auth data provided'],
        syncedRows: {
          users: [{id: 1, name: 'Alice'}],
          posts: [{id: 1, title: 'Post 1'}],
        },
        syncedRowCount: 2,
        start: 1000,
        end: 1150,
        afterPermissions: "users.where('id', 1)",
        vendedRowCounts: {
          users: {
            'SELECT * FROM users WHERE id = ?': 1,
          },
        },
        vendedRows: {
          users: {
            'SELECT * FROM users WHERE id = ?': [{id: 1, name: 'Alice'}],
          },
        },
        sqlitePlans: {
          'SELECT * FROM users WHERE id = ?': [
            'SEARCH users USING INDEX idx_users_id (id=?)',
          ],
        },
      };

      expect(() =>
        valita.parse(complete, analyzeQueryResultSchema),
      ).not.toThrow();
      expect(valita.parse(complete, analyzeQueryResultSchema)).toEqual(
        complete,
      );
    });

    test('validates result with warnings', () => {
      const withWarnings: AnalyzeQueryResult = {
        warnings: [
          'No auth data provided. Permission rules will compare to `NULL` wherever an auth data field is referenced.',
          'Query may be slow due to lack of indexes',
        ],
        syncedRowCount: 5,
        start: 1000,
        end: 1200,
      };

      expect(() =>
        valita.parse(withWarnings, analyzeQueryResultSchema),
      ).not.toThrow();
    });

    test('rejects invalid result with missing required fields', () => {
      const invalid = {
        warnings: [],
        // missing syncedRowCount, start, end
      };

      expect(() => valita.parse(invalid, analyzeQueryResultSchema)).toThrow();
    });

    test('rejects invalid result with wrong types', () => {
      const invalid = {
        warnings: 'not-an-array',
        syncedRowCount: 'not-a-number',
        start: 1000,
        end: 1100,
      };

      expect(() => valita.parse(invalid, analyzeQueryResultSchema)).toThrow();
    });

    test('validates result with zero timing', () => {
      const zeroTiming: AnalyzeQueryResult = {
        warnings: [],
        syncedRowCount: 0,
        start: 0,
        end: 0,
      };

      expect(() =>
        valita.parse(zeroTiming, analyzeQueryResultSchema),
      ).not.toThrow();
    });

    test('validates result with complex query plans', () => {
      const withPlans: AnalyzeQueryResult = {
        warnings: [],
        syncedRowCount: 10,
        start: 1000,
        end: 1050,
        sqlitePlans: {
          'SELECT * FROM users u JOIN posts p ON u.id = p.user_id': [
            'SCAN posts AS p',
            'SEARCH users AS u USING INDEX idx_users_id (id=?)',
          ],
          'SELECT COUNT(*) FROM comments': ['SCAN comments'],
        },
      };

      expect(() =>
        valita.parse(withPlans, analyzeQueryResultSchema),
      ).not.toThrow();
    });
  });

  describe('edge cases', () => {
    test('handles large row counts', () => {
      const largeCount: AnalyzeQueryResult = {
        warnings: [],
        syncedRowCount: Number.MAX_SAFE_INTEGER,
        start: 1000,
        end: 2000,
      };

      expect(() =>
        valita.parse(largeCount, analyzeQueryResultSchema),
      ).not.toThrow();
    });

    test('handles negative timing (edge case)', () => {
      const negativeTiming: AnalyzeQueryResult = {
        warnings: [],
        syncedRowCount: 0,
        start: 1000,
        end: 500, // end before start
      };

      // Schema should still validate even if timing doesn't make sense
      expect(() =>
        valita.parse(negativeTiming, analyzeQueryResultSchema),
      ).not.toThrow();
    });

    test('handles empty nested objects', () => {
      const emptyNested: AnalyzeQueryResult = {
        warnings: [],
        syncedRows: {},
        syncedRowCount: 0,
        start: 1000,
        end: 1100,
        vendedRowCounts: {},
        vendedRows: {},
        sqlitePlans: {},
      };

      expect(() =>
        valita.parse(emptyNested, analyzeQueryResultSchema),
      ).not.toThrow();
    });
  });
});
