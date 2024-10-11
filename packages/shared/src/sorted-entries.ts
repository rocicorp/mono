import {stringCompare} from './string-compare.js';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function sortedEntries<T extends Record<string, any>>(
  object: T,
): [keyof T & string, T[keyof T]][] {
  return Object.entries(object).sort((a, b) => stringCompare(a[0], b[0]));
}
