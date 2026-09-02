import {expect, test} from 'vitest';
import {
  MAX_SCAN,
  Transitions,
  type TransitionValue,
} from './query-transitions.ts';

function make(name: string): {name: string} {
  return {name};
}

const emptyRef = {deref: () => undefined} as unknown as WeakRef<object>;

/** Fills the strong first slot so later stores land in the weak store. */
function fill(t: Transitions<object>): void {
  t.store('first', undefined, undefined, make('first'));
}

/**
 * Values are held weakly, so to simulate collection we swap in an entry whose
 * `WeakRef` is already empty. Only reaches the weak store — the strongly held
 * first child and the bounded store are by definition never collected.
 */
function kill(t: Transitions<object>, key: string, v: TransitionValue): void {
  const byValue = t.rest!.get(key)!;
  const bucket = byValue.get(v)!;
  if (bucket instanceof Set) {
    byValue.set(
      v,
      new Set(Array.from(bucket, e => ({delta: e.delta, ref: emptyRef}))),
    );
  } else {
    byValue.set(v, {delta: bucket.delta, ref: emptyRef});
  }
}

function bucketOf(t: Transitions<object>, key: string, v: TransitionValue) {
  const bucket = t.rest!.get(key)!.get(v);
  expect(bucket).toBeInstanceOf(Set);
  return bucket as Set<{delta: unknown; ref: WeakRef<object>}>;
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
  const t = new Transitions<object>();
  const a = make('a');
  t.store('one', undefined, undefined, a);

  expect(t.first).toBe(a);
  // The whole point: a node with a single child allocates no WeakRef and no Map.
  expect(t.rest).toBe(undefined);
});

test('the value is part of a transition’s identity', () => {
  const t = new Transitions<object>();
  const a = make('a');
  const b = make('b');
  t.store('limit', 10, undefined, a);
  t.store('limit', 20, undefined, b);

  expect(t.lookup('limit', 10, undefined)).toBe(a);
  expect(t.lookup('limit', 20, undefined)).toBe(b);
  expect(t.lookup('limit', 30, undefined)).toBe(undefined);
  // Same key, different value: the first slot must not match on key alone.
  expect(t.first).toBe(a);
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
  const t = new Transitions<object>();
  const a = make('a');
  const b = make('b');
  t.storeBounded('relatedBase:comments', a);
  t.storeBounded('relatedBase:labels', b);

  expect(t.lookupBounded('relatedBase:comments')).toBe(a);
  expect(t.lookupBounded('relatedBase:labels')).toBe(b);
  // Bounded storage is separate: it does not consume the first-child slot, and
  // does not spill into the weak store.
  expect(t.first).toBe(undefined);
  expect(t.rest).toBe(undefined);
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

test('a single-entry bucket stays unwrapped and is promoted on collision', () => {
  const t = new Transitions<object>();
  fill(t);
  t.store('k', undefined, 1, make('a'));
  expect(t.rest!.get('k')!.get(undefined)).not.toBeInstanceOf(Set);
  t.store('k', undefined, 2, make('b'));
  expect(t.rest!.get('k')!.get(undefined)).toBeInstanceOf(Set);
});

test('lookup prunes a collected single entry', () => {
  const t = new Transitions<object>();
  fill(t);
  t.store('k', 'v', undefined, make('a'));
  kill(t, 'k', 'v');

  expect(t.lookup('k', 'v', undefined)).toBe(undefined);
  expect(t.rest!.get('k')!.has('v')).toBe(false);
});

test('lookup prunes collected entries out of a bucket as it walks', () => {
  const t = new Transitions<object>();
  const live = make('live');
  fill(t);
  t.store('k', undefined, 1, make('dead'));
  t.store('k', undefined, 2, live);

  const bucket = bucketOf(t, 'k', undefined);
  const [dead] = [...bucket];
  bucket.delete(dead);
  bucket.add({delta: dead.delta, ref: emptyRef});

  expect(t.lookup('k', undefined, 1)).toBe(undefined);
  expect(bucket.size).toBe(1);
  expect(t.lookup('k', undefined, 2)).toBe(live);
});

test('a hit stops early instead of walking the whole bucket', () => {
  const t = new Transitions<object>();
  fill(t);
  const values = Array.from({length: 10}, (_, i) => make(`v${i}`));
  values.forEach((v, i) => t.store('k', undefined, i, v));

  const bucket = bucketOf(t, 'k', undefined);
  // Kill every entry *after* the one we are about to ask for. A lookup that
  // walked the whole bucket would prune them; stopping at the match leaves them.
  [...bucket].slice(3).forEach(e => {
    bucket.delete(e);
    bucket.add({delta: e.delta, ref: emptyRef});
  });

  expect(t.lookup('k', undefined, 2)).toBe(values[2]);
  expect(bucket.size).toBe(10);
});

test('a bucket stops growing at MAX_SCAN without evicting', () => {
  const t = new Transitions<object>();
  fill(t);
  const values = Array.from({length: MAX_SCAN + 4}, (_, i) => make(`v${i}`));
  values.forEach((v, i) => t.store('k', undefined, i, v));

  expect(bucketOf(t, 'k', undefined).size).toBe(MAX_SCAN);
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
  const t = new Transitions<object>();
  fill(t);

  // Every value distinct and every node collected: the shape of a search box
  // typing into `where:title:=` off a root that never dies. Nothing ever looks
  // these up again, so only a sweep can reclaim them.
  for (let i = 0; i < 1000; i++) {
    t.store('where:title:=', `text-${i}`, undefined, make(`v${i}`));
    t.rest!.get('where:title:=')!.set(`text-${i}`, {
      delta: undefined,
      ref: emptyRef,
    });
  }

  expect(t.restSize).toBeLessThan(200);
  expect(t.rest!.get('where:title:=')!.size).toBeLessThan(200);
});

test('sweeping keeps live entries', () => {
  const t = new Transitions<object>();
  fill(t);
  const live = Array.from({length: 300}, (_, i) => make(`v${i}`));
  live.forEach((v, i) => t.store('k', i, undefined, v));

  expect(t.rest!.get('k')!.size).toBe(300);
  live.forEach((v, i) => expect(t.lookup('k', i, undefined)).toBe(v));
});
