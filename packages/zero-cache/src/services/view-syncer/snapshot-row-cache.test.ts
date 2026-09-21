import {describe, expect, test, vi} from 'vitest';
import {
  DEFAULT_MAX_SNAPSHOT_ROW_CACHE_ENTRIES,
  MAX_INTERNED_STATEMENTS,
  SnapshotRowCache,
} from './snapshot-row-cache.ts';

describe('view-syncer/snapshot-row-cache', () => {
  const SQL = 'SELECT "id","name" FROM "users" WHERE "id"=?';

  test('defaults', () => {
    const cache = new SnapshotRowCache();
    expect(cache.maxEntries).toBe(DEFAULT_MAX_SNAPSHOT_ROW_CACHE_ENTRIES);
    expect(cache.size).toBe(0);
    expect(cache.stats()).toEqual({hits: 0, misses: 0, size: 0});
  });

  test('serves repeated reads from the cache', () => {
    const cache = new SnapshotRowCache(10);
    const row = {id: 1n, name: 'foo'};
    const read = vi.fn(() => row);

    expect(cache.getOrRead('n:01', SQL, 'get', [1n], read)).toBe(row);
    expect(read).toHaveBeenCalledTimes(1);
    expect(cache.stats()).toEqual({hits: 0, misses: 1, size: 1});

    // Same tag, sql and args: served from the cache.
    expect(cache.getOrRead('n:01', SQL, 'get', [1n], read)).toBe(row);
    expect(read).toHaveBeenCalledTimes(1);
    expect(cache.stats()).toEqual({hits: 1, misses: 1, size: 1});
  });

  test('args are compared by value, with bigints equal to numbers', () => {
    const cache = new SnapshotRowCache(10);
    const row = {id: 1n, name: 'foo'};
    const read = vi.fn(() => row);

    expect(cache.getOrRead('n:01', SQL, 'get', [1n], read)).toBe(row);
    // Row keys parsed from the change log are JSON numbers, while values
    // read from SQLite are bigints. Both address the same row.
    expect(cache.getOrRead('n:01', SQL, 'get', [1], read)).toBe(row);
    expect(cache.getOrRead('n:01', SQL, 'get', ['1'], read)).toBe(row);
    expect(read).toHaveBeenCalledTimes(2);
    expect(cache.stats()).toEqual({hits: 1, misses: 2, size: 2});
  });

  test('entries are distinguished by tag, sql and args', () => {
    const cache = new SnapshotRowCache(10);
    let n = 0;
    const read = vi.fn(() => ({n: n++}));

    expect(cache.getOrRead('n:01', SQL, 'get', [1n], read)).toEqual({n: 0});
    expect(cache.getOrRead('n:02', SQL, 'get', [1n], read)).toEqual({n: 1});
    expect(cache.getOrRead('p:01', SQL, 'get', [1n], read)).toEqual({n: 2});
    expect(
      cache.getOrRead('n:01', SQL + ' OR "id"=?', 'get', [1n, 2n], read),
    ).toEqual({n: 3});
    expect(cache.getOrRead('n:01', SQL, 'get', [2n], read)).toEqual({n: 4});
    expect(read).toHaveBeenCalledTimes(5);
    expect(cache.size).toBe(5);

    // Everything is still cached.
    expect(cache.getOrRead('n:01', SQL, 'get', [1n], read)).toEqual({n: 0});
    expect(cache.getOrRead('n:02', SQL, 'get', [1n], read)).toEqual({n: 1});
    expect(cache.getOrRead('p:01', SQL, 'get', [1n], read)).toEqual({n: 2});
    expect(
      cache.getOrRead('n:01', SQL + ' OR "id"=?', 'get', [1n, 2n], read),
    ).toEqual({n: 3});
    expect(cache.getOrRead('n:01', SQL, 'get', [2n], read)).toEqual({n: 4});
    expect(read).toHaveBeenCalledTimes(5);
  });

  test('a tag and sql cannot collide with each other', () => {
    const cache = new SnapshotRowCache(10);
    let n = 0;
    const read = vi.fn(() => ({n: n++}));

    // A malicious-looking arrangement of tag / sql boundaries.
    expect(cache.getOrRead('a', 'b\0c', 'get', [], read)).toEqual({n: 0});
    expect(cache.getOrRead('a\0b', 'c', 'get', [], read)).toEqual({n: 1});
    expect(read).toHaveBeenCalledTimes(2);
  });

  test('undefined (i.e. missing rows) are not cached', () => {
    const cache = new SnapshotRowCache(10);
    const read = vi.fn(() => undefined);

    expect(cache.getOrRead('p:01', SQL, 'get', [1n], read)).toBeUndefined();
    expect(cache.getOrRead('p:01', SQL, 'get', [1n], read)).toBeUndefined();
    expect(read).toHaveBeenCalledTimes(2);
    expect(cache.stats()).toEqual({hits: 0, misses: 2, size: 0});
  });

  test('single-row and multi-row reads of the same statement are distinct', () => {
    // For a table whose only unique key is its primary key, getRow() and
    // getRows() issue the same SQL with the same args and tag.
    const cache = new SnapshotRowCache(10);
    const row = {id: 1n};

    expect(cache.getOrRead('p:01', SQL, 'all', [1n], () => [row])).toEqual([
      row,
    ]);
    expect(cache.getOrRead('p:01', SQL, 'get', [1n], () => row)).toBe(row);
    expect(cache.getOrRead('p:01', SQL, 'all', [1n], () => [])).toEqual([row]);
    expect(cache.stats()).toEqual({hits: 1, misses: 2, size: 2});
  });

  test('empty results (i.e. no conflicting rows) are cached', () => {
    const cache = new SnapshotRowCache(10);
    const read = vi.fn(() => []);

    expect(cache.getOrRead('p:01', SQL, 'all', [1n], read)).toEqual([]);
    expect(cache.getOrRead('p:01', SQL, 'all', [1n], read)).toEqual([]);
    expect(read).toHaveBeenCalledTimes(1);
    expect(cache.stats()).toEqual({hits: 1, misses: 1, size: 1});
  });

  test('evicts the oldest entries first, regardless of hits', () => {
    const cache = new SnapshotRowCache(3);
    const read = (n: number) => () => ({n});

    cache.getOrRead('n:01', SQL, 'get', [1n], read(1));
    cache.getOrRead('n:01', SQL, 'get', [2n], read(2));
    cache.getOrRead('n:01', SQL, 'get', [3n], read(3));
    expect(cache.size).toBe(3);

    // A hit on the oldest entry does not refresh it.
    expect(cache.getOrRead('n:01', SQL, 'get', [1n], read(-1))).toEqual({n: 1});

    // Inserting a fourth entry evicts the oldest (1), not the least
    // recently used (2).
    cache.getOrRead('n:01', SQL, 'get', [4n], read(4));
    expect(cache.size).toBe(3);
    expect(cache.getOrRead('n:01', SQL, 'get', [2n], read(-2))).toEqual({n: 2});
    expect(cache.getOrRead('n:01', SQL, 'get', [3n], read(-3))).toEqual({n: 3});
    expect(cache.getOrRead('n:01', SQL, 'get', [4n], read(-4))).toEqual({n: 4});
    expect(cache.getOrRead('n:01', SQL, 'get', [1n], read(-1))).toEqual({
      n: -1,
    });
    expect(cache.size).toBe(3);
    expect(cache.stats()).toEqual({hits: 4, misses: 5, size: 3});
  });

  test('keeps evicting in insertion order across many wraparounds and clear()', () => {
    const cache = new SnapshotRowCache(3);
    const read = (n: number) => () => ({n});
    const has = (n: number) =>
      cache.getOrRead('n:01', SQL, 'get', [BigInt(n)], () => undefined) !==
      undefined;

    for (let i = 0; i < 10; i++) {
      cache.getOrRead('n:01', SQL, 'get', [BigInt(i)], read(i));
    }
    expect(cache.size).toBe(3);
    expect([6, 7, 8, 9].map(has)).toEqual([false, true, true, true]);

    // After clear(), the cache fills from empty and evicts in order again.
    cache.clear();
    for (let i = 10; i < 15; i++) {
      cache.getOrRead('n:01', SQL, 'get', [BigInt(i)], read(i));
    }
    expect(cache.size).toBe(3);
    expect([11, 12, 13, 14].map(has)).toEqual([false, true, true, true]);
  });

  test('a fractional max size is bounded', () => {
    const cache = new SnapshotRowCache(2.5);
    for (let i = 0; i < 10; i++) {
      cache.getOrRead('n:01', SQL, 'get', [BigInt(i)], () => ({n: i}));
    }
    expect(cache.size).toBe(2);
  });

  test('interned statements are bounded', () => {
    const cache = new SnapshotRowCache(MAX_INTERNED_STATEMENTS * 2);
    const read = vi.fn(() => ({id: 1n}));
    for (let i = 0; i < MAX_INTERNED_STATEMENTS; i++) {
      cache.getOrRead('n:01', `${SQL} -- ${i}`, 'get', [1n], read);
    }
    expect(cache.size).toBe(MAX_INTERNED_STATEMENTS);

    // A known statement does not clear the cache.
    cache.getOrRead('n:01', `${SQL} -- 0`, 'get', [2n], read);
    expect(cache.size).toBe(MAX_INTERNED_STATEMENTS + 1);

    // A new statement beyond the bound starts over.
    cache.getOrRead('n:01', SQL, 'get', [1n], read);
    expect(cache.size).toBe(1);
    expect(read).toHaveBeenCalledTimes(MAX_INTERNED_STATEMENTS + 2);
    cache.getOrRead('n:01', SQL, 'get', [1n], read);
    expect(read).toHaveBeenCalledTimes(MAX_INTERNED_STATEMENTS + 2);
  });

  test('a max size of 0 disables caching', () => {
    const cache = new SnapshotRowCache(0);
    const read = vi.fn(() => ({id: 1n}));

    expect(cache.getOrRead('n:01', SQL, 'get', [1n], read)).toEqual({id: 1n});
    expect(cache.getOrRead('n:01', SQL, 'get', [1n], read)).toEqual({id: 1n});
    expect(read).toHaveBeenCalledTimes(2);
    expect(cache.size).toBe(0);
  });

  test('clear', () => {
    const cache = new SnapshotRowCache(10);
    const read = vi.fn(() => ({id: 1n}));

    cache.getOrRead('n:01', SQL, 'get', [1n], read);
    expect(cache.size).toBe(1);
    cache.clear();
    expect(cache.size).toBe(0);
    cache.getOrRead('n:01', SQL, 'get', [1n], read);
    expect(read).toHaveBeenCalledTimes(2);
  });
});
