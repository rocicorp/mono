import {assert} from 'shared/src/asserts.js';
import type {Ordering, OrderPart, Selector} from '../../ast/ast.js';
import {
  assertStringOrNumber,
  isJoinResult,
  PipelineEntity,
  StringOrNumber,
} from '../types.js';

export function sourcesAreIdentical(
  sourceAName: string,
  sourceAOrder: Ordering,
  sourceBName: string,
  sourceBOrder: Ordering,
) {
  if (sourceAName !== sourceBName) {
    return false;
  }

  return orderingsAreEqual(sourceAOrder, sourceBOrder);
}

export function getCommonPrefixOrdering(
  a: Ordering | undefined,
  b: Ordering | undefined,
): Ordering | undefined {
  if (a === b) {
    return a;
  }
  if (a === undefined || b === undefined) {
    return undefined;
  }
  const minLength = Math.min(a.length, b.length);
  let i = 0;
  for (; i < minLength; ++i) {
    if (a[i][1] !== b[i][1]) {
      break;
    }
  }

  if (i === 0) {
    return undefined;
  }

  const ret: OrderPart[] = [];
  for (let j = 0; j < i; ++j) {
    ret.push(a[i]);
  }
  return ret;
}

export function orderingsAreEqual(
  a: Ordering | undefined,
  b: Ordering | undefined,
) {
  if (a === b) {
    return true;
  }
  if (a === undefined || b === undefined) {
    return false;
  }
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i][1] !== b[i][1]) {
      return false;
    }
    if (!selectorsAreEqual(a[i][0], b[i][0])) {
      return false;
    }
  }
  return true;
}

export function selectorsAreEqual(
  l: readonly [string | null, string],
  r: readonly [string | null, string],
) {
  return l[0] === r[0] && l[1] === r[1];
}

export function selectorArraysAreEqual(
  l: readonly Selector[],
  r: readonly Selector[],
) {
  if (l.length !== r.length) {
    return false;
  }
  return l.every((sel, i) => selectorsAreEqual(sel, r[i]));
}

export function getValueFromEntity(
  entity: Record<string, unknown>,
  qualifiedColumn: readonly [table: string | null, column: string],
): unknown {
  if (isJoinResult(entity) && qualifiedColumn[0] !== null) {
    if (qualifiedColumn[1] === '*') {
      return (entity as Record<string, unknown>)[qualifiedColumn[0]];
    }

    const row = (entity as Record<string, unknown>)[qualifiedColumn[0]];
    if (row === undefined) {
      return undefined;
    }

    return getOrLiftValue(row as Record<string, unknown>, qualifiedColumn[1]);
  }
  return getOrLiftValue(entity, qualifiedColumn[1]);
}

export function getValueFromEntityAsStringOrNumberOrUndefined(
  entity: Record<string, unknown>,
  qualifiedColumn: readonly [table: string | null, column: string],
): StringOrNumber | undefined {
  const value = getValueFromEntity(entity, qualifiedColumn);
  assert(
    typeof value === 'string' ||
      typeof value === 'number' ||
      value === undefined,
  );

  return value;
}

export function getOrLiftValue(
  containerOrValue:
    | Record<string, unknown>
    | Array<Record<string, unknown>>
    | undefined,
  field: string,
) {
  if (Array.isArray(containerOrValue)) {
    return containerOrValue.map(x => x?.[field]);
  }
  return containerOrValue?.[field];
}

export function getPrimaryKeyValuesAsStringUnqualified(
  entity: Record<string, unknown>,
  primaryKey: readonly string[],
) {
  let ret = '';
  let first = true;
  for (const col of primaryKey) {
    if (!first) {
      ret += '-';
    } else {
      first = false;
    }
    ret += entity[col];
  }
  return ret;
}

export function getPrimaryKey(value: PipelineEntity): StringOrNumber {
  // For now only `id` is used for the primary key. We plan to support composite
  // keys in the future.
  const {id} = value;
  assertStringOrNumber(id);
  return id;
}
