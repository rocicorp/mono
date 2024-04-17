import type {StringOrNumber} from './types.js';

export type Entry<T> = readonly [T, Multiplicity];
export type Multiplicity = number;
export type Multiset<T> = Iterable<Entry<T>>;

export function normalize<T>(
  multiset: Multiset<T>,
  getPrimaryKey: (row: T) => StringOrNumber,
) {
  const dedupe = new Map<StringOrNumber, Entry<T>>();
  for (const row of multiset) {
    const key = getPrimaryKey(row[0]);
    const existing = dedupe.get(key);
    if (existing !== undefined) {
      const mult = existing[1] + row[1];
      if (mult === 0) {
        dedupe.delete(key);
      } else {
        dedupe.set(key, [row[0], mult]);
      }
    } else {
      dedupe.set(key, row);
    }
  }

  return dedupe.values();
}
