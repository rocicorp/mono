import type {JSONValue, ReadonlyJSONValue} from 'shared/json.js';
import type {FrozenJSONValue} from './frozen-json.js';
import * as json from 'shared/json.js';
import * as frozenJSON from './frozen-json.js';
import {skipAssertJSONValue, skipFreeze, skipFrozenAsserts} from './config.js';
import type {FrozenCookie, Cookie} from './cookies.js';

export * from 'shared/json.js';
export * from './frozen-json.js';

export function assertJSONValue(v: unknown): asserts v is JSONValue {
  if (skipAssertJSONValue) {
    return;
  }
  return json.assertJSONValue(v);
}

export function assertFrozenJSONValue(
  v: unknown,
): asserts v is FrozenJSONValue {
  if (skipFrozenAsserts || skipAssertJSONValue) {
    return;
  }
  frozenJSON.assertFrozenJSONValue(v);
}

export function assertDeepFrozen<V>(v: V): asserts v is Readonly<V> {
  if (skipFrozenAsserts) {
    return;
  }

  frozenJSON.assertDeepFrozen(v);
}

/**
 * Recursively freezes the passed in value (mutates it) and returns it.
 *
 * This is controlled by `skipFreeze` which is true in release mode.
 */
export function deepFreeze(v: undefined): undefined;
export function deepFreeze(v: Cookie): FrozenCookie;
export function deepFreeze(v: ReadonlyJSONValue): FrozenJSONValue;
export function deepFreeze(
  v: ReadonlyJSONValue | undefined,
): FrozenJSONValue | undefined;
export function deepFreeze(
  v: ReadonlyJSONValue | undefined,
): FrozenJSONValue | undefined {
  if (skipFreeze) {
    return v as FrozenJSONValue | undefined;
  }

  return frozenJSON.deepFreeze(v);
}
