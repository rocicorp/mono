import {deepClone, JSONValue, ReadonlyJSONValue} from './json';
import * as json from './json';
import {
  skipCloneReadTransactionReturnValue,
  skipInternalValueAsserts,
  skipCloneInputValues,
} from './config';

// By using declare we tell the type system that there is a unique symbol.
// However, there is no such symbol but the type system does not care.
declare const internalValueTag: unique symbol;

// We use a class with a private field to prevent plain objects to look like
// InternalValueObjects.
declare class InternalValueObject {
  private [internalValueTag]: true;
}

/**
 * Opaque type representing a JSON value that we store inside Replicache. These
 * must never escape.
 *
 * Example usages of these values are:
 * - B+Tree values
 * - Mutation args
 * - Cookies
 */
export type InternalValue =
  | InternalValueObject
  | string
  | number
  | boolean
  | null;

// We keep track of the internal values for debugging purposes.
const internalValues = new WeakSet();

function isObject(v: unknown): v is object {
  return typeof v === 'object' && v !== null;
}

export function assertInternalValue(v: unknown): asserts v is InternalValue {
  if (skipInternalValueAsserts) {
    return;
  }
  if (isObject(v) && !internalValues.has(v)) {
    throw new Error('Internal value expected');
  }
}

export const enum ToInternalValueReason {
  Test,
  WriteTransactionPut,
  WriteTransactionMutateArgs,

  // The rest skip the clone in release mode. (Controlled by
  // `skipCloneInputValues`
  CookieFromResponse = 0x100,
  ApplyPatch,
}

export function toInternalValue(
  v: ReadonlyJSONValue,
  reason: ToInternalValueReason,
): InternalValue {
  if (skipInternalValueAsserts) {
    if (
      skipCloneInputValues &&
      reason >= ToInternalValueReason.CookieFromResponse
    ) {
      return v as unknown as InternalValue;
    }
    return deepClone(v) as unknown as InternalValue;
  }

  if (isObject(v)) {
    if (internalValues.has(v)) {
      throw new Error('Unexpected internal value');
    }
    const clone = deepClone(v);
    internalValues.add(clone as object);
    return clone as unknown as InternalValue;
  }
  return v as unknown as InternalValue;
}

export function markValueAsInternal(v: ReadonlyJSONValue): void {
  if (!skipInternalValueAsserts && isObject(v)) {
    internalValues.add(v);
  }
}

export const enum FromInternalValueReason {
  Test,
  WatchDiff,
  WriteTransactionScan,
  WriteTransactionGet,
  WriteTransactionMutateArgs,
  PendingMutationGet,
  PersistCompareCookies,

  // The rest skip the clone in release mode (controlled by
  // `skipCloneReadTransactionReturnValue`)
  ReadTransactionScan = 0x100,
  ReadTransactionGet,
}

export function fromInternalValue(
  v: InternalValue,
  reason: FromInternalValueReason,
): JSONValue {
  if (
    skipCloneReadTransactionReturnValue &&
    reason >= FromInternalValueReason.ReadTransactionScan
  ) {
    return v as unknown as JSONValue;
  }
  if (isObject(v)) {
    assertInternalValue(v);
    return deepClone(v as unknown as ReadonlyJSONValue);
  }
  return v as unknown as JSONValue;
}

export function deepEqual(a: InternalValue, b: InternalValue): boolean {
  return json.deepEqual(safeCast(a), safeCast(b));
}

export const enum CastReason {
  EvaluateJSONPointer,
  CookieToRequest,
  CompareCookies,
}

function safeCast(v: InternalValue): ReadonlyJSONValue {
  assertInternalValue(v);
  return v as unknown as ReadonlyJSONValue;
}

export function safeCastToJSON(
  v: InternalValue,
  _castReason: CastReason,
): ReadonlyJSONValue {
  return safeCast(v);
}
