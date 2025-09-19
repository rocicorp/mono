import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AnalyzeQueryResult} from '../../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {analyzeQuery} from './analyze.ts';

// Mock the runAst function
vi.mock('../../../analyze-query/src/run-ast.ts', () => ({
  runAst: vi.fn(),
}));

// Mock the explainQueries function
vi.mock('../../../analyze-query/src/explain-queries.ts', () => ({
  explainQueries: vi.fn(),
}));

// Mock Database
vi.mock('../../../zqlite/src/db.ts', () => ({
  // eslint-disable-next-line @typescript-eslint/naming-convention
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
  // eslint-disable-next-line @typescript-eslint/naming-convention
  MemoryStorage: vi.fn(),
}));

// Mock TableSource
vi.mock('../../../zqlite/src/table-source.ts', () => ({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  TableSource: vi.fn(),
}));

// Mock Debug
vi.mock('../../../zql/src/builder/debug-delegate.ts', () => ({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  Debug: vi.fn(),
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
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;

  const simpleAST: AST = {
    table: 'users',
  };

  test('analyzes basic query with default options', async () => {
    const {runAst} = await import('../../../analyze-query/src/run-ast.ts');
    const {explainQueries} = await import(
      '../../../analyze-query/src/explain-queries.ts'
    );

    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 5,
      start: 1000,
      end: 1050,
      vendedRowCounts: {
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

    const result = await analyzeQuery(lc, mockConfig, simpleAST);

    expect(runAst).toHaveBeenCalledWith(
      lc,
      simpleAST,
      true, // isTransformed
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
    );

    expect(explainQueries).toHaveBeenCalledWith(
      mockResult.vendedRowCounts,
      expect.any(Object),
    );

    expect(result).toEqual({
      ...mockResult,
      plans: mockPlans,
    });
  });

  test('analyzes query with custom options', async () => {
    const {runAst} = await import('../../../analyze-query/src/run-ast.ts');
    const {explainQueries} = await import(
      '../../../analyze-query/src/explain-queries.ts'
    );

    const mockResult: AnalyzeQueryResult = {
      warnings: ['Custom warning'],
      syncedRowCount: 3,
      start: 2000,
      end: 2100,
      vendedRowCounts: {},
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue({});

    const result = await analyzeQuery(lc, mockConfig, simpleAST, {
      syncedRows: false,
      vendedRows: true,
    });

    expect(runAst).toHaveBeenCalledWith(
      lc,
      simpleAST,
      true,
      expect.objectContaining({
        syncedRows: false,
        vendedRows: true,
      }),
    );

    expect(result).toEqual({
      ...mockResult,
      plans: {},
    });
  });

  test('handles query with complex AST', async () => {
    const {runAst} = await import('../../../analyze-query/src/run-ast.ts');

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
      vendedRowCounts: {
        users: {
          'SELECT * FROM users WHERE active = ? ORDER BY name LIMIT ?': 10,
        },
      },
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);

    const result = await analyzeQuery(lc, mockConfig, complexAST);

    expect(runAst).toHaveBeenCalledWith(
      lc,
      complexAST,
      true,
      expect.any(Object),
    );
    expect(result.syncedRowCount).toBe(10);
  });

  test('handles query with no vended row counts', async () => {
    const {runAst} = await import('../../../analyze-query/src/run-ast.ts');
    const {explainQueries} = await import(
      '../../../analyze-query/src/explain-queries.ts'
    );

    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
      vendedRowCounts: undefined,
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue({});

    const result = await analyzeQuery(lc, mockConfig, simpleAST);

    expect(explainQueries).toHaveBeenCalledWith({}, expect.any(Object));
    expect(result.plans).toEqual({});
  });

  test('handles empty vended row counts', async () => {
    const {runAst} = await import('../../../analyze-query/src/run-ast.ts');
    const {explainQueries} = await import(
      '../../../analyze-query/src/explain-queries.ts'
    );

    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
      vendedRowCounts: {},
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);
    vi.mocked(explainQueries).mockReturnValue({});

    const result = await analyzeQuery(lc, mockConfig, simpleAST);

    expect(explainQueries).toHaveBeenCalledWith({}, expect.any(Object));
    expect(result.plans).toEqual({});
  });

  test('propagates errors from runAst', async () => {
    const {runAst} = await import('../../../analyze-query/src/run-ast.ts');

    const error = new Error('Query analysis failed');
    vi.mocked(runAst).mockRejectedValue(error);

    await expect(analyzeQuery(lc, mockConfig, simpleAST)).rejects.toThrow(
      'Query analysis failed',
    );
  });

  test('creates proper host delegate with getSource function', async () => {
    const {runAst} = await import('../../../analyze-query/src/run-ast.ts');
    const {mustGetTableSpec} = await import('../db/lite-tables.ts');
    // eslint-disable-next-line @typescript-eslint/naming-convention
    const {TableSource} = await import('../../../zqlite/src/table-source.ts');

    const mockTableSpec = {
      tableSpec: {primaryKey: ['id']},
      zqlSpec: {},
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    vi.mocked(mustGetTableSpec).mockReturnValue(mockTableSpec as any);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    vi.mocked(TableSource).mockImplementation(() => ({}) as any);

    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);

    await analyzeQuery(lc, mockConfig, simpleAST);

    // Verify that runAst was called with a host that has the expected functions
    const hostArg = vi.mocked(runAst).mock.calls[0][3].host;

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
    );
  });

  test('caches table sources in host delegate', async () => {
    const {runAst} = await import('../../../analyze-query/src/run-ast.ts');
    const {explainQueries} = await import(
      '../../../analyze-query/src/explain-queries.ts'
    );
    const {mustGetTableSpec} = await import('../db/lite-tables.ts');
    // eslint-disable-next-line @typescript-eslint/naming-convention
    const {TableSource} = await import('../../../zqlite/src/table-source.ts');

    const mockTableSpec = {
      tableSpec: {primaryKey: ['id']},
      zqlSpec: {},
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const mockTableSource = {id: 'mock-table-source'} as any;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    vi.mocked(mustGetTableSpec).mockReturnValue(mockTableSpec as any);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    vi.mocked(TableSource).mockImplementation(() => mockTableSource as any);
    vi.mocked(explainQueries).mockReturnValue({});

    const mockResult: AnalyzeQueryResult = {
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
    };

    vi.mocked(runAst).mockResolvedValue(mockResult);

    await analyzeQuery(lc, mockConfig, simpleAST);

    const hostArg = vi.mocked(runAst).mock.calls[0][3].host;

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
    const {runAst} = await import('../../../analyze-query/src/run-ast.ts');

    const options = {
      syncedRows: false,
      vendedRows: true,
    };

    vi.mocked(runAst).mockResolvedValue({
      warnings: [],
      syncedRowCount: 0,
      start: 1000,
      end: 1010,
    });

    await analyzeQuery(lc, mockConfig, simpleAST, options);

    expect(runAst).toHaveBeenCalledWith(
      lc,
      simpleAST,
      true,
      expect.objectContaining(options),
    );
  });
});
