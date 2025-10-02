import {beforeEach, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {hydrate} from '../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {BuilderDelegate} from '../../zql/src/builder/builder.ts';
import {Debug} from '../../zql/src/builder/debug-delegate.ts';
import {Database} from '../../zqlite/src/db.ts';
import {runAst, type RunAstOptions} from './run-ast.ts';

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

  const result1 = await runAst(lc, ast, isTransformed, options1);
  expect(result1).toMatchInlineSnapshot(`
    {
      "afterPermissions": undefined,
      "end": 1011,
      "start": 1010,
      "syncedRowCount": 0,
      "syncedRows": undefined,
      "vendedRowCounts": {
        "users": {
          "SELECT * FROM users": 2,
        },
      },
      "vendedRows": undefined,
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

  const result2 = await runAst(lc, ast, isTransformed, options2);
  expect(result2).toMatchInlineSnapshot(`
    {
      "afterPermissions": undefined,
      "end": 1021,
      "start": 1020,
      "syncedRowCount": 0,
      "syncedRows": undefined,
      "vendedRowCounts": {
        "users": {
          "SELECT * FROM users": 2,
        },
      },
      "vendedRows": {
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

  const result3 = await runAst(lc, ast, isTransformed, options3);
  expect(result3).toMatchInlineSnapshot(`
    {
      "afterPermissions": undefined,
      "end": 1031,
      "start": 1030,
      "syncedRowCount": 0,
      "syncedRows": undefined,
      "vendedRowCounts": {
        "users": {
          "SELECT * FROM users": 2,
        },
      },
      "vendedRows": undefined,
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

  const result = await runAst(lc, ast, isTransformed, options);

  expect(result).toMatchInlineSnapshot(`
    {
      "afterPermissions": undefined,
      "end": 1011,
      "start": 1010,
      "syncedRowCount": 0,
      "syncedRows": undefined,
      "vendedRowCounts": {},
      "vendedRows": undefined,
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

  const result = await runAst(lc, ast, isTransformed, options);

  expect(result).toMatchInlineSnapshot(`
    {
      "afterPermissions": undefined,
      "end": 1011,
      "start": 1010,
      "syncedRowCount": 0,
      "syncedRows": undefined,
      "vendedRowCounts": {
        "users": {
          "SELECT * FROM users": 2,
        },
      },
      "vendedRows": undefined,
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

  const result = await runAst(lc, ast, isTransformed, options);

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

  const result = await runAst(lc, ast, isTransformed, options);

  // Should count only 1 unique row despite 3 identical rows being yielded
  expect(result.syncedRowCount).toBe(1);

  // Verify syncedRows contains only the unique row
  expect(result.syncedRows).toEqual({
    users: [{id: 1, name: 'Alice'}],
  });
});

test('runAst calls Promise.resolve every 10 rows for yielding', async () => {
  // Use fake timers and spy on Promise.resolve
  vi.useFakeTimers();

  // Create a counter for Promise.resolve calls made within the loop
  let promiseResolveCount = 0;
  const originalResolve = Promise.resolve;

  // Mock Promise.resolve to count calls
  vi.spyOn(Promise, 'resolve').mockImplementation((...args) => {
    promiseResolveCount++;
    return originalResolve.apply(Promise, args);
  });

  // Mock hydrate to return exactly 35 rows to test yielding at 10, 20, 30
  vi.mocked(hydrate).mockImplementation(function* () {
    for (let i = 1; i <= 35; i++) {
      yield {
        type: 'add',
        table: 'users',
        queryHash: 'test-hash',
        rowKey: {id: i},
        row: {id: i, name: `User ${i}`},
      };
    }
  });

  const lc = createSilentLogContext();
  const ast: AST = {table: 'users'};
  const isTransformed = true;
  const db = new Database(lc, ':memory:');
  const host = createMockHost(false);

  const options: RunAstOptions = {
    db,
    host,
    tableSpecs: new Map(),
  };

  const runPromise = runAst(lc, ast, isTransformed, options);

  // Advance timers to resolve any pending promises
  await vi.runAllTimersAsync();

  await runPromise;

  // Should have called Promise.resolve at least 3 times (for rows 10, 20, 30)
  // We use >= because there might be other Promise.resolve calls in the system
  expect(promiseResolveCount).toBeGreaterThanOrEqual(3);

  vi.useRealTimers();
});

test('runAst calls setTimeout (via sleep) when processing many rows', async () => {
  // Use fake timers to capture setTimeout calls
  vi.useFakeTimers();
  const setTimeoutSpy = vi.spyOn(global, 'setTimeout');

  // Mock hydrate to return exactly 250 rows to test sleep at 100, 200
  vi.mocked(hydrate).mockImplementation(function* () {
    for (let i = 1; i <= 250; i++) {
      yield {
        type: 'add',
        table: 'users',
        queryHash: 'test-hash',
        rowKey: {id: i},
        row: {id: i, name: `User ${i}`},
      };
    }
  });

  const lc = createSilentLogContext();
  const ast: AST = {table: 'users'};
  const isTransformed = true;
  const db = new Database(lc, ':memory:');
  const host = createMockHost(false);

  const options: RunAstOptions = {
    db,
    host,
    tableSpecs: new Map(),
  };

  const runPromise = runAst(lc, ast, isTransformed, options);

  // Advance timers to resolve any sleep calls
  await vi.runAllTimersAsync();

  await runPromise;

  // Should have called setTimeout with 1ms delay (from sleep(1))
  // Filter for calls with 1ms delay to isolate our sleep calls
  const sleepCalls = setTimeoutSpy.mock.calls.filter(call => call[1] === 1);
  expect(sleepCalls.length).toBeGreaterThanOrEqual(2); // At least at rows 100 and 200

  vi.useRealTimers();
});
