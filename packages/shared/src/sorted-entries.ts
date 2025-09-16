/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-base-to-string, @typescript-eslint/restrict-template-expressions */
import {stringCompare} from './string-compare.ts';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function sortedEntries<T extends Record<string, any>>(
  object: T,
): [keyof T & string, T[keyof T]][] {
  return Object.entries(object).sort((a, b) => stringCompare(a[0], b[0]));
}
