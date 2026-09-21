import {expect, test, vi} from 'vitest';
import {
  SQLiteStoreRead,
  setupDatabase,
  type PreparedStatements,
  type SQLiteDatabase,
} from './sqlite-store.ts';

function makeMockStatements(
  opts: {
    getRows?: unknown[][];
    getManyRows?: unknown[][];
    hasRows?: unknown[][];
    hasManyRows?: unknown[][];
  } = {},
): {
  stmts: PreparedStatements;
  getCallCount: () => number;
  getManyCallCount: () => number;
  hasCallCount: () => number;
  hasManyCallCount: () => number;
} {
  let getCount = 0;
  let getManyCount = 0;
  let hasCount = 0;
  let hasManyCount = 0;

  const stmts: PreparedStatements = {
    put: {async exec() {}, all: () => Promise.resolve([])},
    del: {async exec() {}, all: () => Promise.resolve([])},
    putN: () => ({async exec() {}, all: () => Promise.resolve([])}),
    delN: () => ({async exec() {}, all: () => Promise.resolve([])}),
    get: {
      async exec() {},
      // oxlint-disable-next-line require-await
      async all() {
        getCount++;
        return opts.getRows ?? [];
      },
    },
    has: {
      async exec() {},
      // oxlint-disable-next-line require-await
      async all() {
        hasCount++;
        return opts.hasRows ?? [];
      },
    },
    getMany: {
      async exec() {},
      // oxlint-disable-next-line require-await
      async all() {
        getManyCount++;
        return opts.getManyRows ?? [];
      },
    },
    hasMany: {
      async exec() {},
      // oxlint-disable-next-line require-await
      async all() {
        hasManyCount++;
        return opts.hasManyRows ?? [];
      },
    },
  };

  return {
    stmts,
    getCallCount: () => getCount,
    getManyCallCount: () => getManyCount,
    hasCallCount: () => hasCount,
    hasManyCallCount: () => hasManyCount,
  };
}

test('concurrent gets are batched into a single getMany call', async () => {
  const {stmts, getManyCallCount} = makeMockStatements({
    getManyRows: [
      ['a', '"alpha"'],
      ['b', '"beta"'],
    ],
  });
  const read = new SQLiteStoreRead(() => {}, stmts);

  const [valA, valB, valC] = await Promise.all([
    read.get('a'),
    read.get('b'),
    read.get('c'),
  ]);

  expect(getManyCallCount()).toBe(1);
  expect(valA).toBe('alpha');
  expect(valB).toBe('beta');
  expect(valC).toBeUndefined();
});

test('concurrent has calls are batched into a single hasMany call', async () => {
  const {stmts, hasManyCallCount} = makeMockStatements({
    hasManyRows: [['a']],
  });
  const read = new SQLiteStoreRead(() => {}, stmts);

  const [hasA, hasB] = await Promise.all([read.has('a'), read.has('b')]);

  expect(hasManyCallCount()).toBe(1);
  expect(hasA).toBe(true);
  expect(hasB).toBe(false);
});

test('sequential awaited gets use the single-key fast path', async () => {
  const {stmts, getCallCount, getManyCallCount} = makeMockStatements();
  const read = new SQLiteStoreRead(() => {}, stmts);

  await read.get('a');
  await read.get('b');

  expect(getCallCount()).toBe(2);
  expect(getManyCallCount()).toBe(0);
});

test('mixed concurrent gets and has use separate sql calls', async () => {
  const {stmts, getManyCallCount, hasCallCount} = makeMockStatements();
  const read = new SQLiteStoreRead(() => {}, stmts);

  // Two concurrent gets → getMany; one concurrent has → has (single-key path)
  await Promise.all([read.get('a'), read.has('b'), read.get('c')]);

  expect(getManyCallCount()).toBe(1);
  expect(hasCallCount()).toBe(1);
});

/** Records every statement `setupDatabase` executes, in order. */
function setupAndCollectSQL(): string[] {
  const statements: string[] = [];
  const db: SQLiteDatabase = {
    close: vi.fn(),
    destroy: vi.fn(),
    prepare: vi.fn(() => ({
      exec: () => Promise.resolve(),
      all: () => Promise.resolve([]),
    })),
    execSync: vi.fn((sql: string) => {
      statements.push(sql.trim());
    }),
  };
  setupDatabase(db);
  return statements;
}

/** Records the pragmas `setupDatabase` issues, in order. */
function setupAndCollectPragmas(): string[] {
  const pragmas: string[] = [];
  const db: SQLiteDatabase = {
    close: vi.fn(),
    destroy: vi.fn(),
    prepare: vi.fn(() => ({
      exec: () => Promise.resolve(),
      all: () => Promise.resolve([]),
    })),
    execSync: vi.fn((sql: string) => {
      const match = /^\s*PRAGMA\s+(\w+)/i.exec(sql);
      if (match) {
        pragmas.push(sql.trim());
      }
    }),
  };
  setupDatabase(db);
  return pragmas;
}

function indexOfPragma(pragmas: string[], name: string): number {
  return pragmas.findIndex(p =>
    new RegExp(`^PRAGMA\\s+${name}\\b`, 'i').test(p),
  );
}

test('setupDatabase issues page_size before journal_mode', () => {
  const pragmas = setupAndCollectPragmas();

  const pageSize = indexOfPragma(pragmas, 'page_size');
  const journalMode = indexOfPragma(pragmas, 'journal_mode');

  expect(pageSize).toBeGreaterThanOrEqual(0);
  expect(journalMode).toBeGreaterThanOrEqual(0);
  // SQLite silently ignores page_size once a journal mode has been set, so the
  // order here is load-bearing and not merely stylistic. If this fails, the
  // store is quietly running on 4096.
  expect(pageSize).toBeLessThan(journalMode);
});

test('setupDatabase sets page_size to 8192 and enables mmap', () => {
  const pragmas = setupAndCollectPragmas();

  expect(pragmas[indexOfPragma(pragmas, 'page_size')]).toBe(
    'PRAGMA page_size = 8192',
  );
  expect(pragmas[indexOfPragma(pragmas, 'mmap_size')]).toBe(
    'PRAGMA mmap_size = 268435456',
  );
});

test('setupDatabase creates entry as a rowid table', () => {
  const create = setupAndCollectSQL().find(sql =>
    /^CREATE TABLE IF NOT EXISTS entry\b/i.test(sql),
  );

  expect(create).toBeDefined();
  // Rows are 8-16KB B-tree chunks; WITHOUT ROWID is measurably slower for rows
  // that size. See the comment on the CREATE TABLE in setupDatabase.
  expect(create).not.toMatch(/WITHOUT\s+ROWID/i);
});
