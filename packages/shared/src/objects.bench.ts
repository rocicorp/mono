// How to walk an object's own properties: `Object.entries`, `Object.keys`, or
// `for...in`. Each benchmark walks the same 1000 objects and touches every key
// and value, so the only difference is the iteration strategy.
//
// What it showed on Node 24.18 (V8 13.6):
//
// - A loop that only reads: `Object.keys` + `o[k]` is never slower, and is 3-5x
//   faster than `Object.entries` for small objects. `for...in` + `hasOwn` comes
//   second. Don't use `Object.entries` in hot read-only loops.
// - The same loop inside a generator that yields per property: `Object.keys`
//   is 10-12% faster for small objects, level with `Object.entries` for 32
//   keys, and about 3x faster for dictionary-mode objects. `for...in` is
//   clearly the slowest here.
// - `mapValues`, which also builds an object: `Object.entries` is 3-15% faster
//   on fast-mode objects, `Object.keys` about 2.8x faster on dictionary-mode
//   ones. A wash for the small objects `mapValues` is called with.
//
// Why, from V8's builtins-object-gen.cc: for a fast-mode object with an enum
// cache, `Object.keys` allocates one array and copies the cached keys into it.
// `Object.entries` uses the same cache but allocates a two-element array per
// property, plus the outer array. For dictionary-mode objects both fall back to
// the runtime, and the `Object.entries` fallback is much slower.
//
// Everything is defined in this file on purpose: see the caveat in
// hash.bench.ts about vite turning imported bindings into property lookups.

import {bench, describe, use} from './bench.ts';

type Obj = Record<string, number>;

const COUNT = 1000;

/**
 * - `monomorphic`: every object has the same 3 keys, added in the same order,
 *   so they share one hidden class. Like a node's `relationships`.
 * - `polymorphic`: 8 different 3-key shapes, interleaved.
 * - `megamorphic`: 64 different shapes, interleaved.
 * - `wide`: one shape with 32 keys.
 * - `dictionary`: 32 keys, the first deleted and re-added, which puts the
 *   object in dictionary mode.
 *
 * Checked with `node --allow-natives-syntax`: `%HasFastProperties` is true for
 * all but `dictionary`. Assigning more than about 19 computed keys one by one
 * also ends in dictionary mode, which is why `wide` is built by `JSON.parse`.
 */
const SHAPES: Record<string, () => Obj[]> = {
  monomorphic: () =>
    Array.from({length: COUNT}, (_, i) => ({a: i, b: i + 1, c: i + 2})),
  polymorphic: () => objectsWithShapes(8, 3),
  megamorphic: () => objectsWithShapes(64, 3),
  wide: () => wideObjects(),
  dictionary: () =>
    wideObjects().map(o => {
      const v = o.k0;
      delete o.k0;
      o.k0 = v;
      return o;
    }),
};

function wideObjects(): Obj[] {
  return Array.from({length: COUNT}, (_, i) =>
    JSON.parse(
      JSON.stringify(
        Object.fromEntries(
          Array.from({length: 32}, (_, k) => [`k${k}`, i + k]),
        ),
      ),
    ),
  );
}

function objectsWithShapes(shapes: number, keys: number): Obj[] {
  return Array.from({length: COUNT}, (_, i) => {
    const o: Obj = {};
    const shape = i % shapes;
    for (let k = 0; k < keys; k++) {
      o[`s${shape}_k${k}`] = i + k;
    }
    return o;
  });
}

// Each strategy returns something derived from every key and value, so none
// of the work can be skipped.

function entriesDestructured(o: Obj): number {
  let n = 0;
  for (const [k, v] of Object.entries(o)) {
    n += k.length + v;
  }
  return n;
}

function entriesIndexed(o: Obj): number {
  let n = 0;
  for (const e of Object.entries(o)) {
    n += e[0].length + e[1];
  }
  return n;
}

function keys(o: Obj): number {
  let n = 0;
  for (const k of Object.keys(o)) {
    n += k.length + o[k];
  }
  return n;
}

function keysIndexLoop(o: Obj): number {
  let n = 0;
  const ks = Object.keys(o);
  for (let i = 0; i < ks.length; i++) {
    const k = ks[i];
    n += k.length + o[k];
  }
  return n;
}

function forIn(o: Obj): number {
  let n = 0;
  for (const k in o) {
    if (Object.hasOwn(o, k)) {
      n += k.length + o[k];
    }
  }
  return n;
}

const STRATEGIES: [name: string, walk: (o: Obj) => number][] = [
  ['Object.entries, destructured', entriesDestructured],
  ['Object.entries, indexed', entriesIndexed],
  ['Object.keys + o[k]', keys],
  ['Object.keys, index loop', keysIndexLoop],
  ['for...in + hasOwn', forIn],
];

describe('walk own properties', () => {
  for (const [shape, make] of Object.entries(SHAPES)) {
    const objects = make();
    for (const [name, walk] of STRATEGIES) {
      bench(`${shape} | ${name}`, () => {
        let n = 0;
        for (const o of objects) {
          n += walk(o);
        }
        use(n);
      });
    }
  }
});

// The same walks inside a generator that yields once per property, like
// `Streamer.#streamNodes`: V8 cannot keep the entry arrays out of the heap
// across a yield.

function* genEntries(o: Obj): Generator<number> {
  for (const [k, v] of Object.entries(o)) {
    yield k.length + v;
  }
}

function* genKeys(o: Obj): Generator<number> {
  for (const k of Object.keys(o)) {
    yield k.length + o[k];
  }
}

function* genForIn(o: Obj): Generator<number> {
  for (const k in o) {
    if (Object.hasOwn(o, k)) {
      yield k.length + o[k];
    }
  }
}

const GENERATOR_STRATEGIES: [
  name: string,
  walk: (o: Obj) => Generator<number>,
][] = [
  ['Object.entries, destructured', genEntries],
  ['Object.keys + o[k]', genKeys],
  ['for...in + hasOwn', genForIn],
];

describe('walk own properties in a generator', () => {
  for (const [shape, make] of Object.entries(SHAPES)) {
    const objects = make();
    for (const [name, walk] of GENERATOR_STRATEGIES) {
      bench(`${shape} | ${name}`, () => {
        let n = 0;
        for (const o of objects) {
          for (const x of walk(o)) {
            n += x;
          }
        }
        use(n);
      });
    }
  }
});

// `mapValues` from objects.ts and the same function written with the other
// strategies. `assignProperty` is left out of all of them: it is the same in
// each and only matters for a `__proto__` key.

function mapValuesEntries<U>(o: Obj, f: (v: number) => U): Record<string, U> {
  const out: Record<string, U> = {};
  for (const e of Object.entries(o)) {
    out[e[0]] = f(e[1]);
  }
  return out;
}

function mapValuesKeys<U>(o: Obj, f: (v: number) => U): Record<string, U> {
  const out: Record<string, U> = {};
  for (const k of Object.keys(o)) {
    out[k] = f(o[k]);
  }
  return out;
}

function mapValuesForIn<U>(o: Obj, f: (v: number) => U): Record<string, U> {
  const out: Record<string, U> = {};
  for (const k in o) {
    if (Object.hasOwn(o, k)) {
      out[k] = f(o[k]);
    }
  }
  return out;
}

const double = (v: number) => v * 2;

describe('mapValues', () => {
  for (const [shape, make] of Object.entries(SHAPES)) {
    const objects = make();
    for (const [name, map] of [
      ['Object.entries (current)', mapValuesEntries],
      ['Object.keys', mapValuesKeys],
      ['for...in + hasOwn', mapValuesForIn],
    ] as const) {
      bench(`${shape} | ${name}`, () => {
        for (const o of objects) {
          use(map(o, double));
        }
      });
    }
  }
});
