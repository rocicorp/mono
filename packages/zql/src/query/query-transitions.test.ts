import {expect, test} from 'vitest';
import {HashIndex, MAX_SCAN, Transitions} from './query-transitions.ts';
import {withStubbedWeakRef} from './test/weak-ref-stub.ts';

function make(name: string): {name: string} {
  return {name};
}

/** Fills the strong first slot so later stores land in the weak store. */
function fill(t: Transitions<object>): void {
  t.store('first', undefined, undefined, make('first'));
}

test('round trips a transition', () => {
  const t = new Transitions<object>();
  const a = make('a');
  expect(t.lookup('one', undefined, undefined)).toBe(undefined);
  t.store('one', undefined, undefined, a);
  expect(t.lookup('one', undefined, undefined)).toBe(a);
  expect(t.lookup('other', undefined, undefined)).toBe(undefined);
});

test('the first transition is held strongly, with no weak store at all', () => {
  withStubbedWeakRef(weak => {
    const t = new Transitions<object>();
    const a = make('a');
    t.store('one', undefined, undefined, a);

    // The whole point: a node with a single child allocates no WeakRef.
    expect(weak.created()).toBe(0);
    // ...and holds it strongly, so it outlives collection.
    weak.collect(a);
    expect(t.lookup('one', undefined, undefined)).toBe(a);
  });
});

test('the value is part of a transition’s identity', () => {
  const t = new Transitions<object>();
  const a = make('a');
  const b = make('b');
  t.store('limit', 10, undefined, a);
  t.store('limit', 20, undefined, b);

  expect(t.lookup('limit', 10, undefined)).toBe(a);
  expect(t.lookup('limit', 20, undefined)).toBe(b);
  // Same key, different value: the first slot must not match on key alone.
  expect(t.lookup('limit', 30, undefined)).toBe(undefined);
});

test('values are distinguished by type, not coerced', () => {
  const t = new Transitions<object>();
  const num = make('num');
  const str = make('str');
  t.store('where:id:=', 1, undefined, num);
  t.store('where:id:=', '1', undefined, str);

  expect(t.lookup('where:id:=', 1, undefined)).toBe(num);
  expect(t.lookup('where:id:=', '1', undefined)).toBe(str);
});

test('the first slot still checks its delta', () => {
  const t = new Transitions<object>();
  const a = make('a');
  t.store('k', undefined, {id: 1}, a);

  expect(t.lookup('k', undefined, {id: 1})).toBe(a);
  // Structurally equal but distinct object still matches.
  expect(t.lookup('k', undefined, {id: 1, extra: undefined})).toBe(a);
  // A different delta must not match; it falls through to the empty weak store.
  expect(t.lookup('k', undefined, {id: 2})).toBe(undefined);
});

test('bounded transitions are held strongly and never collected', () => {
  withStubbedWeakRef(weak => {
    const t = new Transitions<object>();
    const a = make('a');
    const b = make('b');
    t.storeBounded('relatedBase:comments', a);
    t.storeBounded('relatedBase:labels', b);

    weak.collect(a);
    weak.collect(b);
    expect(t.lookupBounded('relatedBase:comments')).toBe(a);
    expect(t.lookupBounded('relatedBase:labels')).toBe(b);
    // Bounded storage is separate: no WeakRef for either, and it leaves the
    // first-child slot free for an ordinary transition.
    expect(weak.created()).toBe(0);
    const c = make('c');
    t.store('one', undefined, undefined, c);
    expect(weak.created()).toBe(0);
    weak.collect(c);
    expect(t.lookup('one', undefined, undefined)).toBe(c);
  });
});

test('distinguishes entries in a bucket by deepEqual on the delta', () => {
  const t = new Transitions<object>();
  const a = make('a');
  const b = make('b');
  fill(t);
  t.store('where:tree', undefined, {id: 1}, a);
  t.store('where:tree', undefined, {id: 2}, b);

  expect(t.lookup('where:tree', undefined, {id: 1})).toBe(a);
  expect(t.lookup('where:tree', undefined, {id: 2})).toBe(b);
  expect(t.lookup('where:tree', undefined, {id: 3})).toBe(undefined);
});

test('lookup prunes a collected single entry', () => {
  withStubbedWeakRef(weak => {
    const t = new Transitions<object>();
    fill(t);
    const a = make('a');
    t.store('k', 'v', undefined, a);
    weak.collect(a);

    expect(t.lookup('k', 'v', undefined)).toBe(undefined);
    // The entry is gone rather than merely empty: a second lookup finds
    // nothing left to deref.
    weak.resetDerefs();
    expect(t.lookup('k', 'v', undefined)).toBe(undefined);
    expect(weak.derefs()).toBe(0);
  });
});

test('lookup prunes collected entries out of a bucket as it walks', () => {
  withStubbedWeakRef(weak => {
    const t = new Transitions<object>();
    const dead = make('dead');
    const live = make('live');
    fill(t);
    t.store('k', undefined, 1, dead);
    t.store('k', undefined, 2, live);
    weak.collect(dead);

    expect(t.lookup('k', undefined, 1)).toBe(undefined);
    // Walking past the collected entry dropped it, so finding `live` now
    // derefs one entry rather than two.
    weak.resetDerefs();
    expect(t.lookup('k', undefined, 2)).toBe(live);
    expect(weak.derefs()).toBe(1);
  });
});

test('a hit stops early instead of walking the whole bucket', () => {
  withStubbedWeakRef(weak => {
    const t = new Transitions<object>();
    fill(t);
    const values = Array.from({length: 10}, (_, i) => make(`v${i}`));
    values.forEach((v, i) => t.store('k', undefined, i, v));

    // Entries are walked in insertion order, so the third one is found after
    // three derefs. A lookup that walked the whole bucket first would spend
    // ten.
    weak.resetDerefs();
    expect(t.lookup('k', undefined, 2)).toBe(values[2]);
    expect(weak.derefs()).toBe(3);
  });
});

test('a bucket stops growing at MAX_SCAN without evicting', () => {
  const t = new Transitions<object>();
  fill(t);
  const values = Array.from({length: MAX_SCAN + 4}, (_, i) => make(`v${i}`));
  values.forEach((v, i) => t.store('k', undefined, i, v));

  // Everything that fit stays shared...
  for (let i = 0; i < MAX_SCAN; i++) {
    expect(t.lookup('k', undefined, i)).toBe(values[i]);
  }
  // ...and the surplus is simply not interned, rather than displacing a live
  // entry. This is what keeps a sequential sweep from thrashing to a 0% hit rate.
  for (let i = MAX_SCAN; i < values.length; i++) {
    expect(t.lookup('k', undefined, i)).toBe(undefined);
  }
});

test('a node sweeps dead entries once its weak store has grown', () => {
  withStubbedWeakRef(weak => {
    const t = new Transitions<object>();
    fill(t);

    // Every value distinct and every node collected: the shape of a search box
    // typing into `where:title:=` off a root that never dies. Nothing ever
    // looks these up again, so only a sweep can reclaim them.
    const n = 1000;
    for (let i = 0; i < n; i++) {
      const v = make(`v${i}`);
      t.store('where:title:=', `text-${i}`, undefined, v);
      weak.collect(v);
    }

    // Storing never derefs on its own, so any deref at all is the sweep
    // running -- and its total cost stays proportional to the inserts rather
    // than to the whole map each time, which is what the doubling threshold
    // buys.
    expect(weak.derefs()).toBeGreaterThan(0);
    expect(weak.derefs()).toBeLessThan(n * 4);
    // Nothing collected is handed back.
    for (let i = 0; i < n; i++) {
      expect(t.lookup('where:title:=', `text-${i}`, undefined)).toBe(undefined);
    }
  });
});

test('sweeping keeps live entries', () => {
  withStubbedWeakRef(() => {
    const t = new Transitions<object>();
    fill(t);
    // Enough to cross the sweep threshold several times over.
    const live = Array.from({length: 300}, (_, i) => make(`v${i}`));
    live.forEach((v, i) => t.store('k', i, undefined, v));

    live.forEach((v, i) => expect(t.lookup('k', i, undefined)).toBe(v));
  });
});

test('replace re-points the strong first slot', () => {
  withStubbedWeakRef(weak => {
    const t = new Transitions<object>();
    const from = make('from');
    const to = make('to');
    t.store('k', 1, undefined, from);
    expect(t.replace(from, to)).toBe(true);
    expect(t.lookup('k', 1, undefined)).toBe(to);
    // Still the strong slot afterwards: no WeakRef, and it survives
    // collection.
    expect(weak.created()).toBe(0);
    weak.collect(to);
    expect(t.lookup('k', 1, undefined)).toBe(to);
  });
});

test('replace re-points a weak entry, bare or in a bucket', () => {
  const t = new Transitions<object>();
  fill(t);
  const from = make('from');
  const to = make('to');
  t.store('k', 1, undefined, from);
  expect(t.replace(from, to)).toBe(true);
  expect(t.lookup('k', 1, undefined)).toBe(to);

  const from2 = make('from2');
  const to2 = make('to2');
  const other = make('other');
  t.store('k', 2, {d: 1}, other);
  t.store('k', 2, {d: 2}, from2);
  expect(t.replace(from2, to2)).toBe(true);
  expect(t.lookup('k', 2, {d: 2})).toBe(to2);
  // Replacing one entry of a bucket leaves its neighbour alone.
  expect(t.lookup('k', 2, {d: 1})).toBe(other);
});

test('replace of a node this map never stored is a no-op', () => {
  const t = new Transitions<object>();
  expect(t.replace(make('x'), make('y'))).toBe(false);
  fill(t);
  const stored = make('other');
  t.store('k', 1, undefined, stored);
  expect(t.replace(make('x'), make('y'))).toBe(false);
  // ...and leaves what is stored untouched.
  expect(t.lookup('k', 1, undefined)).toBe(stored);
});

test('a hash index round trips and forgets a collected entry', () => {
  withStubbedWeakRef(({collect}) => {
    const h = new HashIndex<object>();
    const a = make('a');
    h.set('h1', a);
    expect(h.get('h1')).toBe(a);
    expect(h.get('h2')).toBeUndefined();

    collect(a);
    expect(h.get('h1')).toBeUndefined();
    // A collected entry does not shadow a later one under the same hash.
    const b = make('b');
    h.set('h1', b);
    expect(h.get('h1')).toBe(b);
  });
});

test('a hash index keeps live entries through any amount of dead ones', () => {
  withStubbedWeakRef(({collect}) => {
    const h = new HashIndex<object>();
    const live = Array.from({length: 300}, (_, i) => make(`live${i}`));
    live.forEach((v, i) => h.set(`live-${i}`, v));
    // Enough dead inserts to cross several sweeps.
    for (let i = 0; i < 1000; i++) {
      const v = make(`v${i}`);
      h.set(`dead-${i}`, v);
      collect(v);
    }
    live.forEach((v, i) => expect(h.get(`live-${i}`)).toBe(v));
    for (let i = 0; i < 1000; i++) {
      expect(h.get(`dead-${i}`)).toBeUndefined();
    }
  });
});
