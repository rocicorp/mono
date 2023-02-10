import type {IterableUnion} from './iterable-union.js';

/**
 * Filters an async iterable.
 *
 * This utility function is provided because it is useful when using
 * {@link makeScanResult}. It can be used to filter out tombstones (delete entries)
 * for example.
 */
export async function* filterAsyncIterable<V>(
  iter: IterableUnion<V>,
  predicate: (v: V) => boolean,
): AsyncIterable<V> {
  for await (const v of iter) {
    if (predicate(v)) {
      yield v;
    }
  }
}
