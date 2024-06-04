import type {Ordering, Selector} from '../../ast/ast.js';
import {isJoinResult} from '../types.js';

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

export function orderingsAreEqual(a: Ordering, b: Ordering) {
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

export function selectorsAreEqual(l: Selector, r: Selector) {
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
