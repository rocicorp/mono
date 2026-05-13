import {expect, test} from 'vitest';
import {SQLiteStoreRead, type PreparedStatements} from './sqlite-store.ts';

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
