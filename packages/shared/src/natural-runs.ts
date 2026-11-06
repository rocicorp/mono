/**
 * A stable sort for input that already arrives in a handful of ordered runs,
 * for the engines whose built-in sort cannot exploit that.
 *
 * `Array.prototype.sort` is TimSort on V8, so already-sorted input costs N-1
 * comparisons. **On Hermes it is not adaptive**, and the difference is not
 * small. Sorting 100,000 objects through a two-column comparator:
 *
 * ```text
 *                Hermes 0.12                V8
 *   sorted     93.6ms / 1,483,630      0.8ms / 99,999
 *   reversed  168.6ms / 2,571,203      0.8ms / 99,999
 *   shuffled  134.8ms / 1,770,233     17.0ms / 1,529,373
 * ```
 *
 * Hermes does fifteen times the necessary comparisons on sorted input, and
 * reversed input is its *worst* case -- slower than random data, where an O(N)
 * reverse would do. That matters because ordered input is the common case here
 * and not a lucky one: rows come out of a source in primary-key order, and
 * secondary sort keys are usually timestamps or sequence numbers that move with
 * insertion order, so a secondary index is often built over rows that are
 * already sorted, exactly reversed, or a few ordered batches concatenated.
 *
 * This merges the natural runs instead, which is O(N) on one run and
 * O(N log R) on R of them. Measured against the same inputs:
 *
 * ```text
 *                Hermes native -> runs       V8 native -> runs
 *   sorted         93.2ms -> 5.4ms (17x)     1.0ms -> 0.4ms
 *   reversed      166.6ms -> 6.6ms (25x)     1.0ms -> 0.4ms
 *   20 runs       147.2ms -> 9.4ms (16x)     1.0ms -> 0.8ms
 * ```
 *
 * ## Why it gives up rather than always merging
 *
 * A full TimSort would handle near-sorted input too, but without galloping it
 * loses badly to V8's native sort there (8.0ms against 2.8ms on 100,000
 * near-sorted rows, 1.26M comparisons against 268k), and galloping is a large
 * amount of code to carry. So this returns `undefined` once the input has more
 * runs than merging can pay for, and the caller falls back to whatever it would
 * have done -- which keeps V8's galloping and costs Hermes nothing it was not
 * already paying.
 *
 * Declining is not free, and what bounds it is runs, not comparisons: counting
 * stops once a `maxRuns + 1`th run is known to exist, before walking that one,
 * but every run before it is walked to its end. Measured over 100,000 elements
 * with `maxRuns` at 32, against the comparisons the native sort then does:
 *
 * ```text
 *   shuffled                     76 comparisons   0.005%
 *   32 long runs, then more  80,000 comparisons      15%
 * ```
 *
 * The first is the shape that declines in practice, and it gives up almost at
 * once because its runs are two or three elements long. The second is the
 * honest worst case -- `maxRuns` long ordered runs followed by anything at all,
 * where the scan walks most of the array before handing it back. Nothing in the
 * workloads this was built for looks like that, but it is what the bound
 * permits, and a caller sorting adversarial input should know it.
 *
 * ## Stability
 *
 * Stable, and that is load-bearing rather than incidental: callers rely on
 * equal elements keeping their input order. Two rules preserve it. Descending
 * runs are detected **strictly**, so equal neighbours are never part of a run
 * that gets reversed; and the merge takes from the earlier run when its head
 * compares equal. The counting pass also does not mutate, so an array handed
 * back on the `undefined` path is untouched -- reversing runs first and then
 * falling back would hand the caller's sort an array whose equal elements had
 * already been reordered, which is how a stable sort quietly stops being one.
 */

/**
 * Whether this engine's `Array.prototype.sort` already exploits ordered input,
 * measured rather than guessed from the user agent.
 *
 * Sorting an already-sorted array of `PROBE_SIZE` elements takes exactly
 * `n - 1` comparisons on an adaptive sort and several times that otherwise.
 * Measured at `n = 128`: V8 does 127, Hermes does 741. The threshold below sits
 * between them with a wide margin at every size either engine was tried at.
 *
 * This exists so `sortByNaturalRuns` can be skipped where it cannot win. On V8
 * the native sort is already optimal on ordered input, so run detection only
 * adds a JS pass over data a faster native pass would have walked anyway --
 * measured at a 6% regression on cold-start hydration when applied there
 * unconditionally.
 *
 * The probe runs once, costs a few hundred comparisons, and is deliberately not
 * consulted by `sortByNaturalRuns` itself: callers decide, so the sort stays a
 * pure function that can be tested on whichever engine the tests run on.
 */
const PROBE_SIZE = 128;

let adaptive: boolean | undefined;

export function nativeSortIsAdaptive(): boolean {
  if (adaptive === undefined) {
    const probe: number[] = [];
    for (let i = 0; i < PROBE_SIZE; i++) {
      probe.push(i);
    }
    let comparisons = 0;
    probe.sort((a, b) => {
      comparisons++;
      return a - b;
    });
    adaptive = comparisons < PROBE_SIZE * 2;
  }
  return adaptive;
}

/**
 * Sorts `array` in place by merging its natural runs, and returns it.
 *
 * Returns `undefined`, leaving `array` **unmodified**, when it holds more than
 * `maxRuns` runs -- the caller should then sort it however it otherwise would.
 *
 * `compare` must be a consistent comparator; results are stable.
 */
export function sortByNaturalRuns<T>(
  array: T[],
  compare: (a: T, b: T) => number,
  maxRuns = 32,
): T[] | undefined {
  const n = array.length;
  if (n < 2) {
    return array;
  }

  // Pass one, which must not mutate: where each run starts, and whether it is
  // descending and so needs reversing before it can be merged.
  const starts: number[] = [];
  const descending: boolean[] = [];
  let i = 0;
  while (i < n) {
    // Stop before walking another run once `maxRuns` are already in hand and
    // input is left: this call cannot succeed now, and the next run could be
    // the whole remainder of the array.
    if (starts.length === maxRuns) {
      return undefined;
    }
    const start = i;
    let isDescending = false;
    if (i + 1 < n) {
      const first = compare(array[i + 1], array[i]);
      // `c !== c` is a NaN test without a call. A comparator that returns NaN
      // is not an ordering -- it is not even antisymmetric -- so runs and
      // merges are meaningless and, worse, would silently disagree with the
      // caller's own sort: `compareValues` yields NaN for a NaN sort key, and
      // for the values `[1, NaN, 0]` a native sort leaves them alone while
      // merging produces `[0, 1, NaN]`. Decline and let the caller's sort
      // decide, so whatever it does today it keeps doing.
      //
      // This catches a NaN only where this pass happens to compare the key
      // holding it. A multi-column comparator short-circuits on the first
      // differing column, so a NaN in a later column stays invisible here and
      // first surfaces during the merge, which compares rows this pass never
      // put side by side. The merge checks too, and the merge path works on a
      // copy so that it can still decline when it does.
      if (first !== first) {
        return undefined;
      }
      isDescending = first < 0;
      i += 2;
      if (isDescending) {
        // Strictly descending: a run containing equal neighbours must not be
        // reversed, or their relative order would flip.
        while (i < n) {
          const c = compare(array[i], array[i - 1]);
          if (c !== c) {
            return undefined;
          }
          if (c >= 0) {
            break;
          }
          i++;
        }
      } else {
        while (i < n) {
          const c = compare(array[i], array[i - 1]);
          if (c !== c) {
            return undefined;
          }
          if (c < 0) {
            break;
          }
          i++;
        }
      }
    } else {
      i++;
    }
    starts.push(start);
    descending.push(isDescending);
  }
  starts.push(n);

  let runCount = descending.length;
  if (runCount === 1) {
    // One run means no merge, so no comparison beyond the ones already made:
    // nothing further can surface a NaN, and there is nothing to undo. This is
    // also the case worth the most -- already-sorted and exactly-reversed input
    // -- so it stays free of the copy the merge path needs.
    if (descending[0]) {
      reverse(array, 0, n - 1);
    }
    return array;
  }

  // From here the merge compares rows the counting pass never put side by side,
  // which is where a NaN in a later ordering column can first appear. Declining
  // at that point has to leave `array` exactly as it was, so the work happens on
  // a copy and is written back only once it has succeeded.
  let src = array.slice();
  // Pre-sized on purpose: it is filled by index below, and `Array.from` would
  // allocate and then fill it twice.
  // oxlint-disable-next-line unicorn/no-new-array
  let dst = new Array<T>(n);
  let bounds = starts;

  for (let r = 0; r < runCount; r++) {
    if (descending[r]) {
      reverse(src, starts[r], starts[r + 1] - 1);
    }
  }
  while (runCount > 1) {
    const nextBounds: number[] = [0];
    let write = 0;
    let merged = 0;
    for (let p = 0; p < runCount; p += 2) {
      let a = bounds[p];
      const endA = bounds[p + 1];
      if (p + 1 >= runCount) {
        while (a < endA) {
          dst[write++] = src[a++];
        }
      } else {
        let b = bounds[p + 1];
        const endB = bounds[p + 2];
        while (a < endA && b < endB) {
          const c = compare(src[b], src[a]);
          if (c !== c) {
            // A NaN the counting pass could not see. `array` has not been
            // written, so handing it back untouched is still possible.
            return undefined;
          }
          // `< 0` and not `<= 0`: on a tie the earlier run wins, which is what
          // keeps this stable.
          dst[write++] = c < 0 ? src[b++] : src[a++];
        }
        while (a < endA) {
          dst[write++] = src[a++];
        }
        while (b < endB) {
          dst[write++] = src[b++];
        }
      }
      merged++;
      nextBounds.push(write);
    }
    const swap = src;
    src = dst;
    dst = swap;
    bounds = nextBounds;
    runCount = merged;
  }

  for (let c = 0; c < n; c++) {
    array[c] = src[c];
  }
  return array;
}

function reverse<T>(array: T[], lo: number, hi: number): void {
  while (lo < hi) {
    const t = array[lo];
    array[lo++] = array[hi];
    array[hi--] = t;
  }
}
