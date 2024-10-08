import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import {expect, test} from 'vitest';
import {Database} from '../db.js';
import {type CachedStatement, StatementCache} from './statement-cache.js';

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
