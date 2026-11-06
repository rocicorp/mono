import {describe, expect, test} from 'vitest';
import {sortByNaturalRuns} from './natural-runs.ts';

type Item = {k: number; seq: number};

/** `seq` records input position, so stability is observable in the output. */
function items(keys: readonly number[]): Item[] {
  return keys.map((k, seq) => ({k, seq}));
}

const byKey = (a: Item, b: Item) => a.k - b.k;

/** What a stable sort must produce. `Array.prototype.sort` is one. */
function reference(input: readonly Item[]): Item[] {
  return input.toSorted(byKey);
}

/** Sorts, falling back the way a caller would, and returns the result. */
function sortOrFallback(input: Item[], maxRuns?: number): Item[] {
  return sortByNaturalRuns(input, byKey, maxRuns) ?? input.sort(byKey);
}

function lcg(seed: number): () => number {
  return () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };
}

describe('sortByNaturalRuns', () => {
  test.each([
    ['empty', []],
    ['one element', [1]],
    ['two ascending', [1, 2]],
    ['two descending', [2, 1]],
    ['two equal', [1, 1]],
    ['already sorted', [1, 2, 3, 4, 5, 6, 7, 8]],
    ['exactly reversed', [8, 7, 6, 5, 4, 3, 2, 1]],
    ['all equal', [5, 5, 5, 5, 5, 5]],
    ['two runs', [1, 3, 5, 2, 4, 6]],
    ['descending then ascending', [9, 6, 3, 1, 4, 7]],
    ['plateaus', [1, 1, 2, 2, 3, 3, 0, 0]],
  ])('%s matches a stable sort', (_name, keys) => {
    const input = items(keys);
    expect(sortOrFallback(input.slice())).toEqual(reference(input));
  });

  test('is stable across many ties', () => {
    // 50 distinct keys over 2000 elements, so nearly every comparison is a tie.
    const input = items(Array.from({length: 2000}, (_, i) => i % 50));
    const got = sortOrFallback(input.slice());
    expect(got).toEqual(reference(input));
    // Within each key, input order is preserved.
    const tiesOutOfOrder = got.filter(
      (item, i) =>
        i > 0 && item.k === got[i - 1].k && item.seq < got[i - 1].seq,
    );
    expect(tiesOutOfOrder).toEqual([]);
  });

  test('does not reverse a descending run containing equal neighbours', () => {
    // [3, 3, 1] is not strictly descending. Reversing it wholesale would put
    // the second 3 before the first, which a stable sort must not do.
    const input = items([3, 3, 1]);
    const got = sortOrFallback(input.slice());
    expect(got.map(x => x.k)).toEqual([1, 3, 3]);
    expect(got[1].seq).toBe(0);
    expect(got[2].seq).toBe(1);
  });

  test('merges a handful of concatenated sorted batches', () => {
    const keys: number[] = [];
    for (let batch = 0; batch < 8; batch++) {
      for (let i = 0; i < 50; i++) {
        keys.push(batch * 7 + i * 3);
      }
    }
    const input = items(keys);
    const got = sortByNaturalRuns(input.slice(), byKey);
    expect(got).not.toBeUndefined();
    expect(got).toEqual(reference(input));
  });

  test('gives up past maxRuns and leaves the array untouched', () => {
    // Alternating up and down makes a new run every other element.
    const input = items([1, 0, 3, 2, 5, 4, 7, 6, 9, 8]);
    const before = input.slice();
    expect(sortByNaturalRuns(input, byKey, 3)).toBeUndefined();
    expect(input).toEqual(before);
  });

  test('an untouched array still sorts stably afterwards', () => {
    // The point of not mutating before giving up: a caller's own sort must see
    // the original order, or ties come out in the wrong order.
    const keys: number[] = [];
    for (let i = 0; i < 200; i++) {
      keys.push(i % 2 === 0 ? 1 : 0);
    }
    const input = items(keys);
    expect(sortByNaturalRuns(input.slice(), byKey, 4)).toBeUndefined();
    expect(sortOrFallback(input.slice(), 4)).toEqual(reference(input));
  });

  test('matches a stable sort on random inputs', () => {
    const rand = lcg(20261106);
    for (let trial = 0; trial < 200; trial++) {
      const n = Math.floor(rand() * 80);
      // A small key space so ties are common.
      const keys = Array.from({length: n}, () => Math.floor(rand() * 8));
      const input = items(keys);
      expect(sortOrFallback(input.slice())).toEqual(reference(input));
    }
  });

  test('matches a stable sort on partially ordered random inputs', () => {
    const rand = lcg(7);
    for (let trial = 0; trial < 100; trial++) {
      const n = 300;
      const keys = Array.from({length: n}, (_, i) => i);
      // Displace a few elements: enough to break the single run, not always
      // enough to exceed maxRuns, so both paths get exercised.
      const swaps = Math.floor(rand() * 40);
      for (let s = 0; s < swaps; s++) {
        const a = Math.floor(rand() * n);
        const b = Math.floor(rand() * n);
        const t = keys[a];
        keys[a] = keys[b];
        keys[b] = t;
      }
      const input = items(keys);
      expect(sortOrFallback(input.slice())).toEqual(reference(input));
    }
  });

  test('declines when the comparator returns NaN', () => {
    // A NaN-producing comparator is not an ordering, so runs and merges are
    // meaningless -- and would disagree with the caller's own sort rather than
    // just being arbitrary. `[1, NaN, 0]` is the case: a native sort leaves it
    // alone, merging would produce `[0, 1, NaN]`.
    const nanCompare = (a: Item, b: Item) => a.k - b.k;
    for (const keys of [
      [1, Number.NaN, 0],
      [Number.NaN, 1, 0],
      [1, 0, Number.NaN],
      [3, Number.NaN, 2, Number.NaN, 1],
      [Number.NaN, Number.NaN],
    ]) {
      const input = items(keys);
      const before = input.slice();
      expect(sortByNaturalRuns(input, nanCompare)).toBeUndefined();
      expect(input).toEqual(before);
    }
  });

  test('a lone NaN needs no comparison, so it is returned sorted', () => {
    // Nothing is compared in a one-element array, so there is no disagreement
    // to avoid and nothing to decline.
    const input = items([Number.NaN]);
    expect(sortByNaturalRuns(input, (a, b) => a.k - b.k)).toBe(input);
  });

  test('a single NaN does not depend on where it sits', () => {
    // Every element is compared against a neighbour, so the NaN is seen
    // wherever it is -- including at either end.
    for (let at = 0; at < 6; at++) {
      const keys = [0, 1, 2, 3, 4, 5];
      keys[at] = Number.NaN;
      expect(
        sortByNaturalRuns(items(keys), (a, b) => a.k - b.k),
      ).toBeUndefined();
    }
  });

  test('stops scanning once maxRuns runs are known, before walking the next', () => {
    // Four short runs then a long ascending tail, with maxRuns 4. Declining
    // must not drag the tail through the comparator first.
    const keys = [1, 0, 3, 2, 5, 4, 7, 6];
    const tailStart = keys.length;
    for (let i = 0; i < 5000; i++) {
      keys.push(100 + i);
    }
    let comparisons = 0;
    const counting = (a: Item, b: Item) => {
      comparisons++;
      return a.k - b.k;
    };
    expect(sortByNaturalRuns(items(keys), counting, 4)).toBeUndefined();
    // Well under the tail's length: the scan stopped at the run boundary
    // rather than walking 5000 more elements.
    expect(comparisons).toBeLessThan(tailStart * 3);
  });

  describe('NaN in a column the run scan never reaches', () => {
    // A multi-column comparator short-circuits on the first differing column,
    // so a NaN in a later column is invisible while scanning runs and only
    // surfaces during the merge, which compares rows the scan never put side by
    // side. Before this was handled, a random search found ~1,640 divergences
    // from the native sort in 200,000 small cases.
    type Multi = {a: number; v: number; seq: number};
    const multi = (x: Multi, y: Multi) => (x.a !== y.a ? x.a - y.a : x.v - y.v);

    test('the case a single-column guard misses', () => {
      const rows: Multi[] = [
        [1, 1],
        [2, Number.NaN],
        [1, 2],
        [2, 1],
        [1, 1],
        [0, 1],
        [1, Number.NaN],
        [2, 2],
        [1, 0],
      ].map(([a, v], seq) => ({a, v, seq}));
      const before = rows.slice();
      expect(sortByNaturalRuns(rows, multi)).toBeUndefined();
      // Declining happens mid-merge, so the array must still be as handed in.
      expect(rows).toEqual(before);
    });

    test('never disagrees with a stable sort on random multi-column input', () => {
      const rand = lcg(4242);
      const disagreements: unknown[] = [];
      let declines = 0;
      let merges = 0;
      for (let trial = 0; trial < 20000; trial++) {
        const n = 3 + Math.floor(rand() * 7);
        const rows: Multi[] = [];
        for (let i = 0; i < n; i++) {
          rows.push({
            a: Math.floor(rand() * 3),
            v: rand() < 0.3 ? Number.NaN : Math.floor(rand() * 3),
            seq: i,
          });
        }
        const want = rows.toSorted(multi).map(r => r.seq);
        const out = sortByNaturalRuns(rows.slice(), multi);
        if (out === undefined) {
          declines++;
        } else {
          merges++;
          const got = out.map(r => r.seq);
          if (got.join() !== want.join()) {
            disagreements.push({rows: rows.map(r => [r.a, r.v]), got, want});
          }
        }
      }
      expect(disagreements).toEqual([]);
      // Both paths were actually exercised, or the test proves nothing.
      expect(declines).toBeGreaterThan(0);
      expect(merges).toBeGreaterThan(0);
    });
  });

  test('sorts in place and returns the same array', () => {
    const input = items([3, 1, 2]);
    const got = sortByNaturalRuns(input, byKey);
    expect(got).toBe(input);
    expect(input.map(x => x.k)).toEqual([1, 2, 3]);
  });

  describe('the default run budget', () => {
    // sqrt(n), floored at 32. Kept as literals here rather than imported, so a
    // change to the bound has to be restated deliberately.

    /**
     * `runs` ascending runs over `n` elements -- the shape a secondary index
     * arrives in when the primary key leads with a different column. Keys
     * repeat across runs, so ties are common and stability is observable.
     */
    function inRuns(n: number, runs: number): Item[] {
      const per = Math.ceil(n / runs);
      const keys: number[] = [];
      for (let r = 0; r < runs; r++) {
        for (let i = 0; i < per && keys.length < n; i++) {
          keys.push(i);
        }
      }
      return items(keys);
    }

    test('inRuns builds the run count it claims', () => {
      // The other tests here mean nothing if this shape is not what it says.
      for (const runs of [1, 2, 33, 50]) {
        const input = inRuns(4096, runs);
        let got = 1;
        for (let i = 1; i < input.length; i++) {
          if (byKey(input[i], input[i - 1]) < 0) {
            got++;
          }
        }
        expect(got, `runs=${runs}`).toBe(runs);
      }
    });

    test('merges 50 runs, which the old fixed bound of 32 declined', () => {
      const input = inRuns(4096, 50); // budget: sqrt(4096) === 64
      const got = sortByNaturalRuns(input.slice(), byKey);
      expect(got).not.toBeUndefined();
      expect(got).toEqual(reference(input));
      expect(sortByNaturalRuns(input.slice(), byKey, 32)).toBeUndefined();
    });

    test('scales with length: the same run count declines when shorter', () => {
      // 40 runs is inside sqrt(4096) === 64 and outside sqrt(900) === 30,
      // where the floor of 32 governs.
      expect(sortByNaturalRuns(inRuns(4096, 40), byKey)).not.toBeUndefined();
      expect(sortByNaturalRuns(inRuns(900, 40), byKey)).toBeUndefined();
    });

    test('nothing under 1024 elements changes', () => {
      // The floor governs below 1,024, so 32 runs merge and 33 do not.
      expect(sortByNaturalRuns(inRuns(1000, 32), byKey)).not.toBeUndefined();
      expect(sortByNaturalRuns(inRuns(1000, 33), byKey)).toBeUndefined();
    });

    test('admits exactly as many runs as the budget', () => {
      // sqrt(60000) is 244.9, so the budget is 244.
      expect(sortByNaturalRuns(inRuns(60_000, 244), byKey)).not.toBeUndefined();
      expect(sortByNaturalRuns(inRuns(60_000, 245), byKey)).toBeUndefined();
    });

    test('merges stably at every run count the budget admits', () => {
      for (const runs of [1, 2, 31, 32, 33, 64, 100, 244]) {
        const input = inRuns(60_000, runs);
        const got = sortByNaturalRuns(input.slice(), byKey);
        expect(got, `runs=${runs}`).not.toBeUndefined();
        expect(got, `runs=${runs}`).toEqual(reference(input));
      }
    });

    test('still declines shuffled input, and leaves it untouched', () => {
      const rand = lcg(99);
      const keys = Array.from({length: 20_000}, () =>
        Math.floor(rand() * 1_000_000),
      );
      const input = items(keys);
      const before = input.slice();
      expect(sortByNaturalRuns(input, byKey)).toBeUndefined();
      expect(input).toEqual(before);
    });
  });
});
