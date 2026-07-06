import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../db.ts';
import {
  type CachedStatement,
  DEFAULT_MAX_CACHED_STATEMENTS,
  StatementCache,
} from './statement-cache.ts';

test('Same sql results in same statement instance. The same instance is not outstanding twice.', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  const cache = new StatementCache(db);
  const LOOP_COUNT = 100;
  const expected: CachedStatement[] = [];
  for (let i = 0; i < LOOP_COUNT; ++i) {
    const stmt = cache.get(`SELECT ${i}`);
    cache.return(stmt);
    expected.push(stmt);

    expect(cache.size).toBe(expected.length);
  }

  const duplicatedExpected: CachedStatement[] = [];
  for (let i = 0; i < LOOP_COUNT; ++i) {
    // get a statement that is in the cache
    const stmt = cache.get(`SELECT ${i}`);
    // check that it is the one we put in the cache
    expect(stmt.statement).toBe(expected[i].statement);
    expect(cache.size).toBe(expected.length - i - 1);

    // get it again. It is not in the cache now (we have it in hand above)
    // so we should get a new instance.
    const stmt2 = cache.get(`SELECT ${i}`);
    expect(stmt.statement).not.toBe(stmt2.statement);
    duplicatedExpected.push(stmt2);

    // cache size keeps going down until we return the statements
    expect(cache.size).toBe(expected.length - i - 1);
  }

  for (let i = 0; i < LOOP_COUNT; ++i) {
    cache.return(expected[i]);
    expect(cache.size).toBe(i + 1);
  }

  for (let i = 0; i < LOOP_COUNT; ++i) {
    cache.return(duplicatedExpected[i]);
    expect(cache.size).toBe(LOOP_COUNT + i + 1);
  }

  expect(cache.size).toBe(LOOP_COUNT * 2);

  // drops the least recently used LOOP_COUNT statements
  cache.drop(LOOP_COUNT);

  expect(cache.size).toBe(LOOP_COUNT);

  // the most recently used are `duplicatedExpected` and should all be
  // present in the cache
  expect(duplicatedExpected.length).toBe(LOOP_COUNT);
  for (let i = 0; i < LOOP_COUNT * 2; ++i) {
    cache.get(`SELECT ${i % 100}`);
  }

  // all statements are outstanding
  expect(cache.size).toBe(0);
});

test('cache is bounded: returning past maxSize evicts the least recently used statements', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  const cache = new StatementCache(db, 3);
  expect(cache.maxSize).toBe(3);

  const returned: CachedStatement[] = [];
  for (let i = 0; i < 5; ++i) {
    const stmt = cache.get(`SELECT ${i}`);
    cache.return(stmt);
    returned.push(stmt);
    expect(cache.size).toBe(Math.min(i + 1, 3));
  }

  // The 3 most recently used statements are retained and served from the
  // cache (same instance).
  for (let i = 2; i < 5; ++i) {
    expect(cache.get(`SELECT ${i}`).statement).toBe(returned[i].statement);
  }

  // The 2 least recently used statements were evicted, so a fresh
  // statement is prepared.
  for (let i = 0; i < 2; ++i) {
    expect(cache.get(`SELECT ${i}`).statement).not.toBe(returned[i].statement);
  }
});

test('returning a statement refreshes its recency', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  const cache = new StatementCache(db, 3);

  const returned: CachedStatement[] = [];
  for (let i = 0; i < 3; ++i) {
    const stmt = cache.get(`SELECT ${i}`);
    cache.return(stmt);
    returned.push(stmt);
  }

  // Touch 'SELECT 0', making 'SELECT 1' the least recently used.
  cache.use('SELECT 0', () => {});

  // Push the cache over its bound.
  cache.return(cache.get('SELECT 3'));

  expect(cache.size).toBe(3);
  expect(cache.get('SELECT 1').statement).not.toBe(returned[1].statement);
  expect(cache.get('SELECT 0').statement).toBe(returned[0].statement);
  expect(cache.get('SELECT 2').statement).toBe(returned[2].statement);
});

test('a cache hit refreshes the recency of remaining statements for the same sql', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  const cache = new StatementCache(db, 3);

  // Two copies of 'SELECT 0', then 'SELECT 1'. Recency order (LRU first):
  // 'SELECT 0', 'SELECT 1'.
  const a1 = cache.get('SELECT 0');
  const a2 = cache.get('SELECT 0');
  cache.return(a1);
  cache.return(a2);
  const b = cache.get('SELECT 1');
  cache.return(b);
  expect(cache.size).toBe(3);

  // A hit on 'SELECT 0' moves its remaining cached copy to the most
  // recently used position: 'SELECT 1' is now the LRU.
  const gotten = cache.get('SELECT 0');
  expect(cache.size).toBe(2);
  cache.return(gotten);
  expect(cache.size).toBe(3);

  // Push the cache over its bound; 'SELECT 1' is evicted, both copies of
  // 'SELECT 0' are retained.
  cache.return(cache.get('SELECT 2'));
  expect(cache.size).toBe(3);
  expect(cache.get('SELECT 0').statement).toBe(gotten.statement);
  expect(cache.get('SELECT 0').statement).toBe(a1.statement);
  expect(cache.get('SELECT 1').statement).not.toBe(b.statement);
});

test('in-flight statements are never evicted and stay usable', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  const cache = new StatementCache(db, 2);

  // Check out more statements than the cache can hold.
  const outstanding: CachedStatement[] = [];
  for (let i = 0; i < 5; ++i) {
    outstanding.push(cache.get(`SELECT ${i}`));
  }
  // Outstanding statements are not in the cache.
  expect(cache.size).toBe(0);

  // Fill the cache to its bound with other statements; eviction churn must
  // not affect the outstanding statements.
  for (let i = 5; i < 10; ++i) {
    cache.return(cache.get(`SELECT ${i}`));
  }
  expect(cache.size).toBe(2);

  for (let i = 0; i < 5; ++i) {
    expect(outstanding[i].statement.get<Record<string, number>>()).toEqual({
      [`${i}`]: i,
    });
  }

  // Returning the outstanding statements only retains up to the bound.
  for (const stmt of outstanding) {
    cache.return(stmt);
  }
  expect(cache.size).toBe(2);
});

test('duplicate statements for the same sql count against the bound', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  const cache = new StatementCache(db, 1);

  const first = cache.get('SELECT 1');
  const second = cache.get('SELECT 1');
  cache.return(first);
  cache.return(second);

  expect(cache.size).toBe(1);
  // The least recently returned copy was evicted.
  expect(cache.get('SELECT 1').statement).toBe(second.statement);
});

test('the default bound applies when none is given', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  const cache = new StatementCache(db);
  expect(cache.maxSize).toBe(DEFAULT_MAX_CACHED_STATEMENTS);

  for (let i = 0; i < DEFAULT_MAX_CACHED_STATEMENTS + 1; ++i) {
    cache.return(cache.get(`SELECT ${i}`));
  }
  expect(cache.size).toBe(DEFAULT_MAX_CACHED_STATEMENTS);
});
