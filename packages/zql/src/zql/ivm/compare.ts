import {unreachable} from 'shared/src/asserts.js';
import type {Ordering} from '../ast/ast.js';
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
  orderBy: Ordering,
): (l: T, r: T) => number {
  const comparator = (l: T, r: T) => {
    for (const orderPart of orderBy) {
      const comp = compareEntityFields(
        getValueFromEntity(l as Record<string, unknown>, orderPart[0]),
        getValueFromEntity(r as Record<string, unknown>, orderPart[0]),
      );
      if (comp !== 0) {
        return orderPart[1] === 'asc' ? comp : -comp;
      }
    }

    return 0;
  };

  return comparator;
}
