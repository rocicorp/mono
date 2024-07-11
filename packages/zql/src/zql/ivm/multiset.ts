import type {StringOrNumber} from './types.js';

/**
 * A Multiset is a Set where each entry can appear multiple, or even *negative*,
 * times. See: https://en.wikipedia.org/wiki/Multiset for more information.
 *
 * There is a lot packed into this tiny typedef, and it’s at the absolute core
 * of IVM, so buckle up -- this overview is a bit long!
 *
 * We use Multisets to represent changes to data flowing through the pipeline.
 * You can think of a Multiset in the IVM code roughly as a patch: each “entry”
 * represents a row getting added or removed. If the entry has a "multiplicity"
 * of positive 1, it means the row was added. A multiplicity of negative 1 means
 * the row was removed. A multiplicity of 0 conceptually means no change to the
 * row ( e.g., a no-op). This is rarely used but can sometimes happen in
 * internals. Edits are represented by a remove of the old row (-1) followed by
 * an add of the new row (+1).
 *
 * This representation is convenient because it means that operators don’t have
 * to implement add, remove, and edit operations separately. They can define a
 * single "transform" of an input row that handles all three of these cases
 * generically.
 *
 * For example, consider the simplest operator: `filter`. If instead of
 * multiset, we used a traditional patch datastructure with add/edit/remove
 * rows, handling add would be easy: if the filter matches, pass the add patch
 * downstream, otherwise not. Remove would also be easy: put the entire old
 * value in the patch, and if it matches the filter, pass the remove downstream.
 *
 * Edit would be tricky though -- the patch would need to contain both the
 * entire old and new values. Filter would check if both the old value and the
 * new value matched. If the old value matched, but the new value didn't, then
 * the patch should turn into a remove. Similarly if the old value didn't match
 * and the new value did, then the patch should turn into an add. Only if both
 * the old and new values match the filter should it remain an edit.
 *
 * You can see how even for this simple case, representing only adds and removes
 * is much easier. The filter operator can define a single function that matches
 * a row. Surrounding infrastructure can send both remove and add rows through
 * the same filter. Edits are represented by remove/add pairs and all cases are
 * naturally handled.
 *
 * Currently, our Multiset never uses multiplicities other than 1, 0, -1. This
 * is because we are modeling atomic changes to rows which have a unique ID. It
 * doesn’t make sense to remove or add the same row more than once - it can only
 * appear in the output once. Similarly, the same ID should only show up in a
 * Multiset at most twice: once with -1, and once with +1. However IVM is mostly
 * ignorant to this and handles all entries in a Multiset the same way.
 *
 * Finally, we represent our Multiset as an `Iterator` so that it can be lazily
 * computed with JavaScript’s yield. This enables the pipeline to exit early, as
 * in the case of `limit`. It also prevents copying the multiset over and over
 * as it flows through the pipeline.
 */
export type Multiset<T> = Iterable<Entry<T>>;
export type Entry<T> = readonly [T, Multiplicity];
export type Multiplicity = number;

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
