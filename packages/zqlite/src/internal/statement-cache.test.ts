import {expect, test} from 'vitest';
import {CachedStatement, StatementCache} from './statement-cache.js';
import Database from 'better-sqlite3';

test('Same sql results in same statement instance. The same instance is not outstanding twice.', () => {
  const db = new Database(':memory:');
  const cache = new StatementCache(db);

  const expected: CachedStatement[] = [];
  for (let i = 0; i < 100; ++i) {
    const stmt = cache.get(`SELECT ${i}`);
    cache.return(stmt);
    expected.push(stmt);
    expect(cache.size).toBe(expected.length);
  }

  const duplicatedExpected: CachedStatement[] = [];
  for (let i = 0; i < 100; ++i) {
    // get a statement that is in the cache
    const stmt = cache.get(`SELECT ${i}`);
    // check that it is the one we put in the cache
    expect(stmt.statement).toBe(expected[i].statement);

    // get it again. It is not in the cache now (we have it in hand above)
    // so we should get a new instance.
    const stmt2 = cache.get(`SELECT ${i}`);
    expect(stmt.statement).not.toBe(stmt2.statement);
    duplicatedExpected.push(stmt2);

    // cache size keeps going down until we return the statements
    expect(cache.size).toBe(expected.length - i - 1);
  }

  for (let i = 0; i < 100; ++i) {
    cache.return(expected[i]);
    expect(cache.size).toBe(i + 1);
  }
  for (let i = 0; i < 100; ++i) {
    cache.return(duplicatedExpected[i]);
    expect(cache.size).toBe(100 + i + 1);
  }

  // drops the least recently used 100 statements
  cache.drop(100);

  expect(cache.size).toBe(100);
  // the most recently used are `duplicatedExpected` and should all be
  // present in the cache
  for (let i = 0; i < 100; ++i) {
    const stmt = cache.get(`SELECT ${i}`);
    expect(stmt.statement).toBe(duplicatedExpected[i].statement);
  }

  // all statements are outstanding
  expect(cache.size).toBe(0);
});
