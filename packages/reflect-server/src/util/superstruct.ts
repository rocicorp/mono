import type * as s from 'superstruct';
import {assert} from 'superstruct';

export function superstructAssert<T, S>(
  value: unknown,
  struct: s.Struct<T, S>,
): asserts value is T {
  if (typeof MINIFLARE !== 'undefined') {
    // TODO(greg): figure out how to detect when running
    // on `wrangler dev` and assert there as well.
    assert(value, struct);
  }
}

export function superstructAssertMapValues<T, S>(
  map: Map<string, unknown>,
  struct: s.Struct<T, S>,
): asserts map is Map<string, T> {
  for (const [, value] of map) {
    superstructAssert(value, struct);
  }
}
