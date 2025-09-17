import {beforeEach, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
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
