import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {
  AnalyzeQueryResult,
  PlanDebugEventJSON,
} from '../../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {
  AccumulatorDebugger,
  serializePlanDebugEvents,
} from '../../../zql/src/planner/planner-debug.ts';
import {explainQueries} from '../../../zqlite/src/explain-queries.ts';
import {createSQLiteCostModel} from '../../../zqlite/src/sqlite-cost-model.ts';
import {TableSource} from '../../../zqlite/src/table-source.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {mustGetTableSpec} from '../db/lite-tables.ts';
import {analyzeQuery} from './analyze.ts';
import {runAst} from './run-ast.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';

// Mock the runAst function
vi.mock('./run-ast.ts', () => ({
  runAst: vi.fn(),
}));

// Mock the explainQueries function
vi.mock('../../../zqlite/src/explain-queries.ts', () => ({
  explainQueries: vi.fn(),
}));

// Mock Database
vi.mock('../../../zqlite/src/db.ts', () => ({
  Database: class {
    [Symbol.dispose]() {}
  },
}));

// Mock computeZqlSpecs
vi.mock('../db/lite-tables.ts', () => ({
  computeZqlSpecs: vi.fn(),
  mustGetTableSpec: vi.fn(),
}));

// Mock MemoryStorage
vi.mock('../../../zql/src/ivm/memory-storage.ts', () => ({
  MemoryStorage: vi.fn(),
}));

// Mock TableSource
vi.mock('../../../zqlite/src/table-source.ts', () => ({
  TableSource: vi.fn(),
}));

// Mock Debug
vi.mock('../../../zql/src/builder/debug-delegate.ts', () => ({
  Debug: vi.fn(),
}));

// Mock planner debug and cost model
vi.mock('../../../zql/src/planner/planner-debug.ts', () => ({
  AccumulatorDebugger: vi.fn(() => ({
    events: [],
  })),
  serializePlanDebugEvents: vi.fn(events => events),
}));

vi.mock('../../../zqlite/src/sqlite-cost-model.ts', () => ({
  createSQLiteCostModel: vi.fn(() => ({
    // Mock cost model function
  })),
}));

describe('analyzeQuery', () => {
  const lc = createSilentLogContext();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  const mockConfig: NormalizedZeroConfig = {
    replica: {
      file: '/path/to/replica.db',
    },
    log: {
      level: 'error',
    },
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;

  const minimalClientSchema: ClientSchema = {tables: {}};

  const simpleAST: AST = {
    table: 'users',
  };

  test('analyzes basic query with default options', async () => {
    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 5,
      start: 1000,
      end: 1050,
      readRowCountsByQuery: {
        users: {
          'SELECT * FROM users': 5,
        },
      },
    };

    const mockPlans = {
      'SELECT * FROM users': ['SCAN users'],
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue(mockPlans);

    const result = await analyzeQuery(
      lc,
      mockConfig,
      minimalClientSchema,
      simpleAST,
    );

    expect(runAst).toHaveBeenCalledWith(
      lc,
      minimalClientSchema,
      simpleAST,
      true, // isTransformed (AST already has server names)
      expect.objectContaining({
        applyPermissions: false,
        syncedRows: true,
        vendedRows: false,
        db: expect.any(Object),
        tableSpecs: expect.any(Map),
        host: expect.objectContaining({
          debug: expect.any(Object),
          getSource: expect.any(Function),
          createStorage: expect.any(Function),
          decorateSourceInput: expect.any(Function),
          decorateInput: expect.any(Function),
          addEdge: expect.any(Function),
          decorateFilterInput: expect.any(Function),
        }),
      }),
      expect.anything(),
    );

    expect(explainQueries).toHaveBeenCalledWith(
      mockResult.readRowCountsByQuery,
      expect.any(Object),
    );

    expect(result).toEqual({
      ...mockResult,
      sqlitePlans: mockPlans,
    });
  });

  test('analyzes query with custom options', async () => {
    const mockResult: AnalyzeQueryResult = {
      warnings: ['Custom warning'],
      syncedRowCount: 3,
      start: 2000,
      end: 2100,
      readRowCountsByQuery: {},
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue({});

    const result = await analyzeQuery(
      lc,
      mockConfig,
      minimalClientSchema,
      simpleAST,
      false,
      true,
    );

    expect(runAst).toHaveBeenCalledWith(
      lc,
      minimalClientSchema,
      simpleAST,
      true,
      expect.objectContaining({
        syncedRows: false,
        vendedRows: true,
      }),
      expect.anything(),
    );

    expect(result).toEqual({
      ...mockResult,
      sqlitePlans: {},
    });
  });

  test('handles query with complex AST', async () => {
    const complexAST: AST = {
      table: 'users',
      where: {
        type: 'simple',
        left: {type: 'column', name: 'active'},
        op: '=',
        right: {type: 'literal', value: true},
      },
      orderBy: [['name', 'asc']],
      limit: 10,
    };

    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 10,
      start: 1500,
      end: 1600,
      readRowCountsByQuery: {
        users: {
          'SELECT * FROM users WHERE active = ? ORDER BY name LIMIT ?': 10,
        },
      },
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);

    const result = await analyzeQuery(
      lc,
      mockConfig,
      minimalClientSchema,
      complexAST,
    );

    expect(runAst).toHaveBeenCalledWith(
      lc,
      minimalClientSchema,
      complexAST,
      true,
      expect.any(Object),
      expect.anything(),
    );
    expect(result.syncedRowCount).toBe(10);
  });

  test('handles query with no read row counts by query', async () => {
    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
      readRowCountsByQuery: undefined,
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue({});

    const result = await analyzeQuery(
      lc,
      mockConfig,
      minimalClientSchema,
      simpleAST,
    );

    expect(explainQueries).toHaveBeenCalledWith({}, expect.any(Object));
    expect(result.sqlitePlans).toEqual({});
  });

  test('handles empty read row counts by query', async () => {
    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
      readRowCountsByQuery: {},
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue({});

    const result = await analyzeQuery(
      lc,
      mockConfig,
      minimalClientSchema,
      simpleAST,
    );

    expect(explainQueries).toHaveBeenCalledWith({}, expect.any(Object));
    expect(result.sqlitePlans).toEqual({});
  });

  test('propagates errors from runAst', async () => {
    const error = new Error('Query analysis failed');
    vi.mocked(runAst).mockRejectedValue(error);

    await expect(
      analyzeQuery(lc, mockConfig, minimalClientSchema, simpleAST),
    ).rejects.toThrow('Query analysis failed');
  });

  test('creates proper host delegate with getSource function', async () => {
    const mockTableSpec = {
      tableSpec: {primaryKey: ['id']},
      zqlSpec: {},
    };

    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    vi.mocked(mustGetTableSpec).mockReturnValue(mockTableSpec as any);
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    vi.mocked(TableSource).mockImplementation(() => ({}) as any);

    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);

    await analyzeQuery(lc, mockConfig, minimalClientSchema, simpleAST);

    // Verify that runAst was called with a host that has the expected functions
    const hostArg = vi.mocked(runAst).mock.calls[0][4].host;

    expect(typeof hostArg.getSource).toBe('function');
    expect(typeof hostArg.createStorage).toBe('function');
    expect(typeof hostArg.decorateSourceInput).toBe('function');
    expect(typeof hostArg.decorateInput).toBe('function');
    expect(typeof hostArg.addEdge).toBe('function');
    expect(typeof hostArg.decorateFilterInput).toBe('function');
    expect(hostArg.debug).toBeDefined();

    // Test the getSource function
    const tableName = 'test_table';
    hostArg.getSource(tableName);

    expect(mustGetTableSpec).toHaveBeenCalledWith(expect.any(Map), tableName);
    expect(TableSource).toHaveBeenCalledWith(
      lc,
      mockConfig.log,
      expect.any(Object), // db
      tableName,
      mockTableSpec.zqlSpec,
      mockTableSpec.tableSpec.primaryKey,
      expect.anything(), // should yield
    );
  });

  test('caches table sources in host delegate', async () => {
    const mockTableSpec = {
      tableSpec: {primaryKey: ['id']},
      zqlSpec: {},
    };

    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    const mockTableSource = {id: 'mock-table-source'} as any;

    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    vi.mocked(mustGetTableSpec).mockReturnValue(mockTableSpec as any);
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    vi.mocked(TableSource).mockImplementation(() => mockTableSource as any);
    vi.mocked(explainQueries).mockReturnValue({});

    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);

    await analyzeQuery(lc, mockConfig, minimalClientSchema, simpleAST);

    const hostArg = vi.mocked(runAst).mock.calls[0][4].host;

    // Call getSource twice with the same table name
    const tableName = 'test_table';
    const source1 = hostArg.getSource(tableName);
    const source2 = hostArg.getSource(tableName);

    // Should return the same cached instance
    expect(source1).toBe(source2);
    expect(source1).toBe(mockTableSource);

    // TableSource constructor should only be called once
    expect(TableSource).toHaveBeenCalledTimes(1);
  });

  test('passes through all analyze options correctly', async () => {
    vi.mocked(runAst).mockResolvedValue({
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
    });

    await analyzeQuery(
      lc,
      mockConfig,
      minimalClientSchema,
      simpleAST,
      false,
      true,
    );

    expect(runAst).toHaveBeenCalledWith(
      lc,
      minimalClientSchema,
      simpleAST,
      true,
      expect.objectContaining({
        syncedRows: false,
        vendedRows: true,
      }),
      expect.anything(),
    );
  });

  test('uses readRowCountsByQuery not deprecated vendedRowCounts for explain queries', async () => {
    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 5,
      start: 1000,
      end: 1050,
      // Only set the new property, not the deprecated one
      readRowCountsByQuery: {
        users: {
          'SELECT * FROM users': 5,
        },
      },
      // Deprecated property should not be used even if present
      vendedRowCounts: {
        users: {
          'SELECT * FROM users': 99,
        },
      },
    };

    const mockPlans = {
      'SELECT * FROM users': ['SCAN users'],
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue(mockPlans);

    await analyzeQuery(lc, mockConfig, minimalClientSchema, simpleAST);

    // Verify explainQueries is called with readRowCountsByQuery, not vendedRowCounts
    expect(explainQueries).toHaveBeenCalledWith(
      mockResult.readRowCountsByQuery,
      expect.any(Object),
    );

    // Verify it's NOT called with the deprecated property
    expect(explainQueries).not.toHaveBeenCalledWith(
      mockResult.vendedRowCounts,
      expect.any(Object),
    );
  });

  test('sqlitePlans are populated when readRowCountsByQuery is set (regression test)', async () => {
    // This test simulates the actual bug: vendedRowCounts was deprecated and no longer set,
    // but the code was using it. When readRowCountsByQuery is undefined, explainQueries
    // would be called with undefined/empty object, resulting in no sqlitePlans.
    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 10,
      start: 1000,
      end: 1050,
      readRowCountsByQuery: {
        issues: {
          'SELECT * FROM issues WHERE id = ?': 10,
        },
      },
      // vendedRowCounts is undefined (as it would be from runAst after deprecation)
      vendedRowCounts: undefined,
    };

    const expectedPlans = {
      'SELECT * FROM issues WHERE id = ?': [
        'SCAN issues',
        'USING INDEX idx_issues_id',
      ],
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue(expectedPlans);

    const result = await analyzeQuery(
      lc,
      mockConfig,
      minimalClientSchema,
      simpleAST,
    );

    // Critical assertion: explainQueries must be called with readRowCountsByQuery
    // If it were called with vendedRowCounts (undefined), we'd get no sqlitePlans
    expect(explainQueries).toHaveBeenCalledWith(
      mockResult.readRowCountsByQuery,
      expect.any(Object),
    );

    // Verify sqlitePlans are actually populated in the result
    expect(result.sqlitePlans).toEqual(expectedPlans);
    expect(Object.keys(result.sqlitePlans ?? {})).toHaveLength(1);
  });

  test('sqlitePlans default to empty object when readRowCountsByQuery is undefined', async () => {
    // Edge case: when readRowCountsByQuery is undefined, we should default to {}
    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
      readRowCountsByQuery: undefined,
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue({});

    const result = await analyzeQuery(
      lc,
      mockConfig,
      minimalClientSchema,
      simpleAST,
    );

    // Should call explainQueries with empty object (due to ?? {} in the code)
    expect(explainQueries).toHaveBeenCalledWith({}, expect.any(Object));
    expect(result.sqlitePlans).toEqual({});
  });

  test('real integration: explainQueries produces actual sqlitePlans from readRowCountsByQuery', async () => {
    // This test bypasses the mock for explainQueries to verify real plan generation
    const {explainQueries: realExplainQueries} = await vi.importActual<
      // oxlint-disable-next-line consistent-type-imports
      typeof import('../../../zqlite/src/explain-queries.ts')
    >('../../../zqlite/src/explain-queries.ts');
    const {Database: RealDatabase} = await vi.importActual<
      // oxlint-disable-next-line consistent-type-imports
      typeof import('../../../zqlite/src/db.ts')
    >('../../../zqlite/src/db.ts');

    using db = new RealDatabase(lc, ':memory:');

    // Create a test table with an index
    db.exec(`
      CREATE TABLE users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE
      );
      CREATE INDEX idx_users_email ON users(email);
    `);

    // Create readRowCountsByQuery data
    const readRowCountsByQuery = {
      users: {
        'SELECT * FROM users': 10,
        'SELECT * FROM users WHERE email = ?': 1,
      },
    };

    // Call the real explainQueries function
    const plans = realExplainQueries(readRowCountsByQuery, db);

    // Verify plans were generated
    expect(Object.keys(plans)).toHaveLength(2);
    expect(plans).toHaveProperty('SELECT * FROM users');
    expect(plans).toHaveProperty('SELECT * FROM users WHERE email = ?');

    // Verify plans contain actual SQLite EXPLAIN QUERY PLAN output
    const fullScanPlan = plans['SELECT * FROM users'];
    expect(fullScanPlan.length).toBeGreaterThan(0);
    // SQLite plans should mention SCAN
    expect(fullScanPlan.some(line => line.includes('SCAN'))).toBe(true);

    const indexQueryPlan = plans['SELECT * FROM users WHERE email = ?'];
    expect(indexQueryPlan.length).toBeGreaterThan(0);
    // This query should use an index (SEARCH) or do a SCAN
    expect(
      indexQueryPlan.some(
        line => line.includes('SCAN') || line.includes('SEARCH'),
      ),
    ).toBe(true);
  });

  test('result includes elapsed time (regression for elapsed/end deprecation)', async () => {
    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 5,
      start: 1000,
      end: 1050,
      elapsed: 50,
      readRowCountsByQuery: {
        users: {
          'SELECT * FROM users': 5,
        },
      },
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue({});

    const result = await analyzeQuery(
      lc,
      mockConfig,
      minimalClientSchema,
      simpleAST,
    );

    // Verify elapsed is present (new property)
    expect(result.elapsed).toBe(50);

    // Verify elapsed matches end - start
    expect(result.elapsed).toBe(result.end - result.start);

    // Verify deprecated 'end' is still present for backward compatibility
    expect(result.end).toBe(1050);
    expect(result.start).toBe(1000);
  });

  test('elapsed is calculated correctly when present', async () => {
    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 10,
      start: 2000,
      end: 2150,
      elapsed: 150,
      readRowCountsByQuery: {},
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue({});

    const result = await analyzeQuery(
      lc,
      mockConfig,
      minimalClientSchema,
      simpleAST,
    );

    expect(result.elapsed).toBe(150);
    expect(result.elapsed).toBe(result.end - result.start);
  });

  describe('planner debug', () => {
    test('includes join plans when joinPlans is true', async () => {
      const mockDebugEvents = [
        {type: 'attempt-start', attemptNumber: 0, totalAttempts: 1} as const,
        {
          type: 'plan-complete',
          attemptNumber: 0,
          totalCost: 10.5,
          flipPattern: 0,
          joinStates: [],
        } as const,
      ];

      const mockDebugger = {
        events: mockDebugEvents,
        format: vi.fn().mockReturnValue('formatted debug output'),
      };

      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      vi.mocked(AccumulatorDebugger).mockReturnValue(mockDebugger as any);
      vi.mocked(serializePlanDebugEvents).mockReturnValue(
        mockDebugEvents as unknown as PlanDebugEventJSON[],
      );

      const mockResult: AnalyzeQueryResult = {
        warnings: [],
        syncedRowCount: 5,
        start: 1000,
        end: 1050,
        readRowCountsByQuery: {},
      };

      vi.mocked(runAst).mockResolvedValue(mockResult);
      vi.mocked(explainQueries).mockReturnValue({});

      const result = await analyzeQuery(
        lc,
        mockConfig,
        minimalClientSchema,
        simpleAST,
        true,
        false,
        undefined,
        undefined,
        true, // joinPlans = true
      );

      // Verify AccumulatorDebugger was created
      expect(AccumulatorDebugger).toHaveBeenCalled();

      // Verify createSQLiteCostModel was called
      expect(createSQLiteCostModel).toHaveBeenCalledWith(
        expect.any(Object),
        expect.any(Map),
      );

      // Verify runAst was called with cost model and debugger
      expect(runAst).toHaveBeenCalledWith(
        lc,
        minimalClientSchema,
        simpleAST,
        true,
        expect.objectContaining({
          costModel: expect.anything(),
          planDebugger: mockDebugger,
        }),
        expect.anything(), // shouldYield
      );

      // Verify serializePlanDebugEvents was called
      expect(serializePlanDebugEvents).toHaveBeenCalledWith(mockDebugEvents);

      // Verify result includes joinPlans
      expect(result.joinPlans).toEqual(mockDebugEvents);
    });

    test('does not include join plans when joinPlans is false', async () => {
      vi.clearAllMocks();

      const mockResult: AnalyzeQueryResult = {
        warnings: [],
        syncedRowCount: 5,
        start: 1000,
        end: 1050,
        readRowCountsByQuery: {},
      };

      vi.mocked(runAst).mockResolvedValue(mockResult);
      vi.mocked(explainQueries).mockReturnValue({});

      const result = await analyzeQuery(
        lc,
        mockConfig,
        minimalClientSchema,
        simpleAST,
        true,
        false,
        undefined,
        undefined,
        false, // joinPlans = false
      );

      // Verify AccumulatorDebugger was NOT created
      expect(AccumulatorDebugger).not.toHaveBeenCalled();

      // Verify createSQLiteCostModel was NOT called
      expect(createSQLiteCostModel).not.toHaveBeenCalled();

      // Verify runAst was called without cost model and debugger
      expect(runAst).toHaveBeenCalledWith(
        lc,
        minimalClientSchema,
        simpleAST,
        true,
        expect.objectContaining({
          costModel: undefined,
          planDebugger: undefined,
        }),
        expect.anything(), // shouldYield
      );

      // Verify serializePlanDebugEvents was NOT called
      expect(serializePlanDebugEvents).not.toHaveBeenCalled();

      // Verify result does not include joinPlans
      expect(result.joinPlans).toBeUndefined();
    });

    test('defaults joinPlans to false when not provided', async () => {
      vi.clearAllMocks();

      const mockResult: AnalyzeQueryResult = {
        warnings: [],
        syncedRowCount: 5,
        start: 1000,
        end: 1050,
        readRowCountsByQuery: {},
      };

      vi.mocked(runAst).mockResolvedValue(mockResult);
      vi.mocked(explainQueries).mockReturnValue({});

      // Call without joinPlans parameter
      const result = await analyzeQuery(
        lc,
        mockConfig,
        minimalClientSchema,
        simpleAST,
      );

      // Verify cost model and debugger were NOT created
      expect(AccumulatorDebugger).not.toHaveBeenCalled();
      expect(createSQLiteCostModel).not.toHaveBeenCalled();

      // Verify result does not include joinPlans
      expect(result.joinPlans).toBeUndefined();
    });
  });
});
