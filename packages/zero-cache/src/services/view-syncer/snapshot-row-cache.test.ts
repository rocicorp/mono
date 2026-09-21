import {describe, expect, test, vi} from 'vitest';
import {
  DEFAULT_MAX_SNAPSHOT_ROW_CACHE_ENTRIES,
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

    expect(cache.getOrRead('n:01', SQL, [1n], read)).toBe(row);
    expect(read).toHaveBeenCalledTimes(1);
    expect(cache.stats()).toEqual({hits: 0, misses: 1, size: 1});

    // Same tag, sql and args: served from the cache.
    expect(cache.getOrRead('n:01', SQL, [1n], read)).toBe(row);
    expect(read).toHaveBeenCalledTimes(1);
    expect(cache.stats()).toEqual({hits: 1, misses: 1, size: 1});
  });

  test('args are compared by value, with bigints equal to numbers', () => {
    const cache = new SnapshotRowCache(10);
    const row = {id: 1n, name: 'foo'};
    const read = vi.fn(() => row);

    expect(cache.getOrRead('n:01', SQL, [1n], read)).toBe(row);
    // Row keys parsed from the change log are JSON numbers, while values
    // read from SQLite are bigints. Both address the same row.
    expect(cache.getOrRead('n:01', SQL, [1], read)).toBe(row);
    expect(cache.getOrRead('n:01', SQL, ['1'], read)).toBe(row);
    expect(read).toHaveBeenCalledTimes(2);
    expect(cache.stats()).toEqual({hits: 1, misses: 2, size: 2});
  });

  test('entries are distinguished by tag, sql and args', () => {
    const cache = new SnapshotRowCache(10);
    let n = 0;
    const read = vi.fn(() => ({n: n++}));

    expect(cache.getOrRead('n:01', SQL, [1n], read)).toEqual({n: 0});
    expect(cache.getOrRead('n:02', SQL, [1n], read)).toEqual({n: 1});
    expect(cache.getOrRead('p:01', SQL, [1n], read)).toEqual({n: 2});
    expect(cache.getOrRead('n:01', SQL + ' OR "id"=?', [1n, 2n], read)).toEqual(
      {n: 3},
    );
    expect(cache.getOrRead('n:01', SQL, [2n], read)).toEqual({n: 4});
    expect(read).toHaveBeenCalledTimes(5);
    expect(cache.size).toBe(5);

    // Everything is still cached.
    expect(cache.getOrRead('n:01', SQL, [1n], read)).toEqual({n: 0});
    expect(cache.getOrRead('n:02', SQL, [1n], read)).toEqual({n: 1});
    expect(cache.getOrRead('p:01', SQL, [1n], read)).toEqual({n: 2});
    expect(cache.getOrRead('n:01', SQL + ' OR "id"=?', [1n, 2n], read)).toEqual(
      {n: 3},
    );
    expect(cache.getOrRead('n:01', SQL, [2n], read)).toEqual({n: 4});
    expect(read).toHaveBeenCalledTimes(5);
  });

  test('a tag and sql cannot collide with each other', () => {
    const cache = new SnapshotRowCache(10);
    let n = 0;
    const read = vi.fn(() => ({n: n++}));

    // A malicious-looking arrangement of tag / sql boundaries.
    expect(cache.getOrRead('a', 'b\0c', [], read)).toEqual({n: 0});
    expect(cache.getOrRead('a\0b', 'c', [], read)).toEqual({n: 1});
    expect(read).toHaveBeenCalledTimes(2);
  });

  test('undefined (i.e. missing rows) are not cached', () => {
    const cache = new SnapshotRowCache(10);
    const read = vi.fn(() => undefined);

    expect(cache.getOrRead('p:01', SQL, [1n], read)).toBeUndefined();
    expect(cache.getOrRead('p:01', SQL, [1n], read)).toBeUndefined();
    expect(read).toHaveBeenCalledTimes(2);
    expect(cache.stats()).toEqual({hits: 0, misses: 2, size: 0});
  });

  test('empty results (i.e. no conflicting rows) are cached', () => {
    const cache = new SnapshotRowCache(10);
    const read = vi.fn(() => []);

    expect(cache.getOrRead('p:01', SQL, [1n], read)).toEqual([]);
    expect(cache.getOrRead('p:01', SQL, [1n], read)).toEqual([]);
    expect(read).toHaveBeenCalledTimes(1);
    expect(cache.stats()).toEqual({hits: 1, misses: 1, size: 1});
  });

  test('evicts the oldest entries first, regardless of hits', () => {
    const cache = new SnapshotRowCache(3);
    const read = (n: number) => () => ({n});

    cache.getOrRead('n:01', SQL, [1n], read(1));
    cache.getOrRead('n:01', SQL, [2n], read(2));
    cache.getOrRead('n:01', SQL, [3n], read(3));
    expect(cache.size).toBe(3);

    // A hit on the oldest entry does not refresh it.
    expect(cache.getOrRead('n:01', SQL, [1n], read(-1))).toEqual({n: 1});

    // Inserting a fourth entry evicts the oldest (1), not the least
    // recently used (2).
    cache.getOrRead('n:01', SQL, [4n], read(4));
    expect(cache.size).toBe(3);
    expect(cache.getOrRead('n:01', SQL, [2n], read(-2))).toEqual({n: 2});
    expect(cache.getOrRead('n:01', SQL, [3n], read(-3))).toEqual({n: 3});
    expect(cache.getOrRead('n:01', SQL, [4n], read(-4))).toEqual({n: 4});
    expect(cache.getOrRead('n:01', SQL, [1n], read(-1))).toEqual({n: -1});
    expect(cache.size).toBe(3);
    expect(cache.stats()).toEqual({hits: 4, misses: 5, size: 3});
  });

  test('a max size of 0 disables caching', () => {
    const cache = new SnapshotRowCache(0);
    const read = vi.fn(() => ({id: 1n}));

    expect(cache.getOrRead('n:01', SQL, [1n], read)).toEqual({id: 1n});
    expect(cache.getOrRead('n:01', SQL, [1n], read)).toEqual({id: 1n});
    expect(read).toHaveBeenCalledTimes(2);
    expect(cache.size).toBe(0);
  });

  test('clear', () => {
    const cache = new SnapshotRowCache(10);
    const read = vi.fn(() => ({id: 1n}));

    cache.getOrRead('n:01', SQL, [1n], read);
    expect(cache.size).toBe(1);
    cache.clear();
    expect(cache.size).toBe(0);
    cache.getOrRead('n:01', SQL, [1n], read);
    expect(read).toHaveBeenCalledTimes(2);
  });
});
