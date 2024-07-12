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
 * the row was removed. A multiplicity of 0 means no change to the row (i.e., a
 * no-op -- This is rarely used but can sometimes happen in internals). Edits
 * are represented by a remove of the old row (-1) followed by an add of the new
 * row (+1).
 *
 * Other multiplicities are allowed and just mean more or fewer adds or
 * additions of the same row. It's also fine to have multiple entries for the
 * same row. These three multisets all mean the same thing:
 *
 *  - [[r1, 1], [r1, 1]]
 *  - [[r1, 2]]
 *  - [[r1, 3], [r1, -1]]
 *
 * Note that when we say "row" here, we're talking about a *version* of a row
 * (aka a "tuple"). Not a row as identified by an ID. It's commonly the case for
 * two different versions of a row to be in a multiset at once: for example the
 * old row and the new row during an edit. These would both have the same ID,
 * but different fields contents.
 *
 * This representation of change is convenient for a few reasons:
 *
 * First, it means that operators don’t have to implement add, remove, and edit
 * operations separately. They can define a single "transform" of an input row
 * that handles all three of these cases generically.
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
 * The other advantage of the Multiset representation is that makes processing
 * incremental changes purely functional. The remove and add entries for a row
 * don't have to appear in the set in sequence. Thus we can process the entries
 * in any order or even in parallel.
 *
 * Finally, we represent our Multiset as a JavaScript `Iterator` so that it can
 * be lazily computed with yield. This enables the pipeline to exit early, as in
 * the case of `limit`. It also prevents copying the multiset over and over as
 * it flows through the pipeline.
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
