import {unreachable} from 'shared/src/asserts.js';
import type {Ordering} from '../ast/ast.js';
import {getValueFromEntity} from './source/util.js';
import {compareUTF8} from 'compare-utf8';

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
  if (typeof lVal === 'string') {
    // We compare all strings in zql as UTF-8. If we were only dealing with zql
    // on both client and server we could choose any ordering we want. But we
    // also need to consider that we sometimes ask other systems to sort for
    // us - specifically the database backing the source. So we need to choose
    // a sort that database can do too. UTF-8 is a commonly used collation and
    // the default encoding of many systems (though sadly not javascript).
    // For more, see: https://blog.replicache.dev/blog/replicache-11-adventures-in-text-encoding.
    return compareUTF8(lVal, rVal as string);
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
