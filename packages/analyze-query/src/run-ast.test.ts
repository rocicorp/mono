import {beforeEach, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {hydrate} from '../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {BuilderDelegate} from '../../zql/src/builder/builder.ts';
import {Debug} from '../../zql/src/builder/debug-delegate.ts';
import {Database} from '../../zqlite/src/db.ts';
import {runAst, type RunAstOptions} from './run-ast.ts';
import type {ClientSchema} from '../../zero-protocol/src/client-schema.ts';

const minimalClientSchema: ClientSchema = {tables: {}};

// Mock only the complex dependencies that require extensive setup
vi.mock('../../zero-cache/src/services/view-syncer/pipeline-driver.ts', () => ({
  hydrate: vi.fn(function* () {
    // Return no rows for simplicity
  }),
}));

vi.mock('../../zql/src/builder/builder.ts', () => ({
  buildPipeline: vi.fn(() => ({})),
}));

// Create a minimal host that mimics the BuilderDelegate interface
function createMockHost(withDebug = false): BuilderDelegate {
  const baseHost: Omit<BuilderDelegate, 'debug'> = {
    getSource: vi.fn(),
    createStorage: vi.fn(),
    decorateInput: vi.fn(),
    addEdge: vi.fn(),
    decorateFilterInput: vi.fn(),
    decorateSourceInput: vi.fn(),
  };

  if (withDebug) {
    const debug = new Debug();
    debug.initQuery('users', 'SELECT * FROM users');
    debug.rowVended('users', 'SELECT * FROM users', {id: 1, name: 'Alice'});
    debug.rowVended('users', 'SELECT * FROM users', {id: 2, name: 'Bob'});

    return {...baseHost, debug};
  }

  return {...baseHost, debug: undefined};
}

beforeEach(() => {
  // Clear all mocks before each test
  vi.clearAllMocks();

  // Reset the hydrate mock to return no rows by default
  vi.mocked(hydrate).mockImplementation(function* () {
    // Return no rows for simplicity by default
  });

  // Mock performance.now to return predictable values
  let performanceNowCounter = 1000;
  vi.spyOn(performance, 'now').mockImplementation(
    () => performanceNowCounter++,
  );

  return () => {
    vi.restoreAllMocks();
  };
});

test('runAst always returns vendedRowCounts regardless of vendedRows option', async () => {
  const lc = createSilentLogContext();
  const ast: AST = {
    table: 'users',
  };
  const isTransformed = true;
  const db = new Database(lc, ':memory:');

  const host = createMockHost(true);

  // Test 1: vendedRows option is false - vendedRowCounts should still be populated
  const options1: RunAstOptions = {
    db,
    host,
    tableSpecs: new Map(),
    vendedRows: false,
  };

  const result1 = await runAst(
    lc,
    minimalClientSchema,
    ast,
    isTransformed,
    options1,
  );
  expect(result1).toMatchInlineSnapshot(`
    {
      "afterPermissions": undefined,
      "end": 1003,
      "readRowCount": 2,
      "readRowCountsByQuery": {
        "users": {
          "SELECT * FROM users": 2,
        },
      },
      "readRows": undefined,
      "start": 1002,
      "syncedRowCount": 0,
      "syncedRows": undefined,
      "warnings": [],
    }
  `);

  // Test 2: vendedRows option is true - both should be populated
  const options2: RunAstOptions = {
    db,
    host,
    tableSpecs: new Map(),
    vendedRows: true,
  };

  const result2 = await runAst(
    lc,
    minimalClientSchema,
    ast,
    isTransformed,
    options2,
  );
  expect(result2).toMatchInlineSnapshot(`
    {
      "afterPermissions": undefined,
      "end": 1005,
      "readRowCount": 2,
      "readRowCountsByQuery": {
        "users": {
          "SELECT * FROM users": 2,
        },
      },
      "readRows": {
        "users": {
          "SELECT * FROM users": [
            {
              "id": 1,
              "name": "Alice",
            },
            {
              "id": 2,
              "name": "Bob",
            },
          ],
        },
      },
      "start": 1004,
      "syncedRowCount": 0,
      "syncedRows": undefined,
      "warnings": [],
    }
  `);

  // Test 3: vendedRows option is undefined - vendedRowCounts should still be populated
  const options3: RunAstOptions = {
    db,
    host,
    tableSpecs: new Map(),
    // vendedRows not specified
  };

  const result3 = await runAst(
    lc,
    minimalClientSchema,
    ast,
    isTransformed,
    options3,
  );
  expect(result3).toMatchInlineSnapshot(`
    {
      "afterPermissions": undefined,
      "end": 1007,
      "readRowCount": 2,
      "readRowCountsByQuery": {
        "users": {
          "SELECT * FROM users": 2,
        },
      },
      "readRows": undefined,
      "start": 1006,
      "syncedRowCount": 0,
      "syncedRows": undefined,
      "warnings": [],
    }
  `);
});

test('runAst returns empty object for vendedRowCounts when no debug tracking', async () => {
  const lc = createSilentLogContext();
  const ast: AST = {
    table: 'users',
  };
  const isTransformed = true;
  const db = new Database(lc, ':memory:');

  const host = createMockHost(false); // No debug

  const options: RunAstOptions = {
    db,
    host,
    tableSpecs: new Map(),
  };

  const result = await runAst(
    lc,
    minimalClientSchema,
    ast,
    isTransformed,
    options,
  );

  expect(result).toMatchInlineSnapshot(`
    {
      "afterPermissions": undefined,
      "end": 1003,
      "readRowCount": 0,
      "readRowCountsByQuery": {},
      "readRows": undefined,
      "start": 1002,
      "syncedRowCount": 0,
      "syncedRows": undefined,
      "warnings": [],
    }
  `);
});

test('runAst basic structure and functionality', async () => {
  const lc = createSilentLogContext();
  const ast: AST = {
    table: 'users',
  };
  const isTransformed = true;
  const db = new Database(lc, ':memory:');

  const host = createMockHost(true);

  const options: RunAstOptions = {
    db,
    host,
    tableSpecs: new Map(),
  };

  const result = await runAst(
    lc,
    minimalClientSchema,
    ast,
    isTransformed,
    options,
  );

  expect(result).toMatchInlineSnapshot(`
    {
      "afterPermissions": undefined,
      "end": 1003,
      "readRowCount": 2,
      "readRowCountsByQuery": {
        "users": {
          "SELECT * FROM users": 2,
        },
      },
      "readRows": undefined,
      "start": 1002,
      "syncedRowCount": 0,
      "syncedRows": undefined,
      "warnings": [],
    }
  `);
});

test('runAst counts only unique synced rows, skips duplicates', async () => {
  // Mock hydrate to return both unique and duplicate rows
  vi.mocked(hydrate).mockImplementation(function* () {
    // First unique row from users table
    yield {
      type: 'add',
      table: 'users',
      queryHash: 'test-hash',
      rowKey: {id: 1},
      row: {id: 1, name: 'Alice'},
    };

    // Second unique row from users table
    yield {
      type: 'add',
      table: 'users',
      queryHash: 'test-hash',
      rowKey: {id: 2},
      row: {id: 2, name: 'Bob'},
    };

    // Duplicate of first row (same table + row content)
    yield {
      type: 'add',
      table: 'users',
      queryHash: 'test-hash',
      rowKey: {id: 1},
      row: {id: 1, name: 'Alice'},
    };

    // Unique row from different table
    yield {
      type: 'add',
      table: 'posts',
      queryHash: 'test-hash',
      rowKey: {id: 1},
      row: {id: 1, title: 'Post 1'},
    };

    // Duplicate of the posts row
    yield {
      type: 'add',
      table: 'posts',
      queryHash: 'test-hash',
      rowKey: {id: 1},
      row: {id: 1, title: 'Post 1'},
    };

    // Another unique row from users (different content)
    yield {
      type: 'add',
      table: 'users',
      queryHash: 'test-hash',
      rowKey: {id: 3},
      row: {id: 3, name: 'Charlie'},
    };
  });

  const lc = createSilentLogContext();
  const ast: AST = {
    table: 'users',
  };
  const isTransformed = true;
  const db = new Database(lc, ':memory:');
  const host = createMockHost(false);

  const options: RunAstOptions = {
    db,
    host,
    tableSpecs: new Map(),
    syncedRows: true, // Enable to verify syncedRows also deduplicates
  };

  const result = await runAst(
    lc,
    minimalClientSchema,
    ast,
    isTransformed,
    options,
  );

  // Should count only 4 unique rows: 3 from users table, 1 from posts table
  // Duplicates should be skipped
  expect(result.syncedRowCount).toBe(4);

  // Verify syncedRows also contains deduplicated data
  expect(result.syncedRows).toEqual({
    users: [
      {id: 1, name: 'Alice'},
      {id: 2, name: 'Bob'},
      {id: 3, name: 'Charlie'},
    ],
    posts: [{id: 1, title: 'Post 1'}],
  });
});

test('runAst handles case where all synced rows are duplicates', async () => {
  // Mock hydrate to return only duplicate rows
  vi.mocked(hydrate).mockImplementation(function* () {
    const sameRow = {id: 1, name: 'Alice'};

    // Same row yielded multiple times
    yield {
      type: 'add',
      table: 'users',
      queryHash: 'test-hash',
      rowKey: {id: 1},
      row: sameRow,
    };

    yield {
      type: 'add',
      table: 'users',
      queryHash: 'test-hash',
      rowKey: {id: 1},
      row: sameRow,
    };

    yield {
      type: 'add',
      table: 'users',
      queryHash: 'test-hash',
      rowKey: {id: 1},
      row: sameRow,
    };
  });

  const lc = createSilentLogContext();
  const ast: AST = {
    table: 'users',
  };
  const isTransformed = true;
  const db = new Database(lc, ':memory:');
  const host = createMockHost(false);

  const options: RunAstOptions = {
    db,
    host,
    tableSpecs: new Map(),
    syncedRows: true,
  };

  const result = await runAst(
    lc,
    minimalClientSchema,
    ast,
    isTransformed,
    options,
  );

  // Should count only 1 unique row despite 3 identical rows being yielded
  expect(result.syncedRowCount).toBe(1);

  // Verify syncedRows contains only the unique row
  expect(result.syncedRows).toEqual({
    users: [{id: 1, name: 'Alice'}],
  });
});
