import {unreachable} from 'shared/src/asserts.js';
import type {Selector} from '../ast/ast.js';
import {getValueFromEntity} from './source/util.js';

export function compareEntityFields<T>(lVal: T, rVal: T) {
  if (lVal === rVal) {
    return 0;
  }
  if (lVal === null || lVal === undefined) {
    return -1;
  }
  if (rVal === null || rVal === undefined) {
    return 1;
  }
  if (lVal < rVal) {
    return -1;
  }
  if (lVal > rVal) {
    return 1;
  }

  if (lVal instanceof Date && rVal instanceof Date) {
    return lVal.getTime() - rVal.getTime();
  }

  unreachable();
}

export function makeComparator<T extends object>(
  qualifiedColumns: readonly Selector[],
  direction: 'asc' | 'desc',
): (l: T, r: T) => number {
  const comparator = (l: T, r: T) => {
    let comp = 0;
    for (const qualifiedColumn of qualifiedColumns) {
      comp = compareEntityFields(
        getValueFromEntity(l as Record<string, unknown>, qualifiedColumn),
        getValueFromEntity(r as Record<string, unknown>, qualifiedColumn),
      );
      if (comp !== 0) {
        return comp;
      }
    }

    return comp;
  };

  return direction === 'asc' ? comparator : (l, r) => comparator(r, l);
}
