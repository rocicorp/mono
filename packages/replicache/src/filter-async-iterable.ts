/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
import type {IterableUnion} from './iterable-union.ts';

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
