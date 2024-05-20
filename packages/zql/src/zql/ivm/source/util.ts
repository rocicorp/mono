import {must} from 'shared/src/must.js';
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

  if (sourceAOrder[0].length !== sourceBOrder[0].length) {
    return false;
  }

  if (sourceAOrder[1] !== sourceBOrder[1]) {
    return false;
  }

  return sourceAOrder[0].every((col, i) =>
    selectorsAreEqual(sourceBOrder[0][i], col),
  );
}

export function selectorsAreEqual(l: Selector, r: Selector) {
  return l[0] === r[0] && l[1] === r[1];
}

export function getValueFromEntity(
  entity: Record<string, unknown>,
  qualifiedColumn: readonly [table: string | null, column: string],
) {
  if (isJoinResult(entity) && qualifiedColumn[0] !== null) {
    if (qualifiedColumn[1] === '*') {
      return (entity as Record<string, unknown>)[qualifiedColumn[0]];
    }
    return getOrLiftValue(
      (entity as Record<string, unknown>)[must(qualifiedColumn[0])] as Record<
        string,
        unknown
      >,
      qualifiedColumn[1],
    );
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
