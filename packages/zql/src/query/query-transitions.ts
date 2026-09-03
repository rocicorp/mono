import {deepEqual, type ReadonlyJSONValue} from '../../../shared/src/json.ts';

/**
 * A transition tree for interning immutable builder objects, modelled on V8's
 * hidden-class transitions.
 *
 * Each node keeps a map from a *transition* (an operation plus its arguments) to
 * the node that operation produces. Applying the same operation to the same node
 * twice therefore yields the same object both times.
 *
 * ## Why not hash?
 *
 * Hashing the resulting AST would work but is far too expensive to do on every
 * builder call. Instead a transition is identified by three things, in
 * decreasing order of how cheap they are to compare:
 *
 * 1. `key` — a short string naming the operation. Callers are expected to hand
 *    in a *stable* string rather than build one per call; see below.
 * 2. `value` — the operation's varying argument, when it is a primitive, used
 *    directly as a `Map` key.
 * 3. `delta` — anything left over, compared with {@linkcode deepEqual}.
 *
 * Splitting 1 from 2 is what makes this fast. Folding the value into the key
 * instead means building a fresh string on every call, and a fresh string has to
 * be hashed from scratch before a `Map` can look it up: measured at ~148ns,
 * against ~10ns to look up the caller's own string, whose hash V8 has already
 * cached on the string object. That difference is larger than everything else a
 * transition does.
 *
 * Comparing only the delta is sound because the parent node is already canonical:
 * two children of the same parent are equal exactly when the operations that
 * produced them are equal. We never deep-compare a whole AST.
 *
 * ## Liveness
 *
 * Children point back at their parents strongly (see `QueryImpl`), so holding a
 * query holds its whole ancestor spine and re-deriving it from the root gives
 * the same object back. The other direction has to be weak, or an interned root
 * — which lives as long as its schema — would retain every query ever derived
 * from it.
 *
 * Weak is expensive, though: `new WeakRef` costs on the order of a hundred
 * nanoseconds, more than everything else a transition does put together, and it
 * adds GC work later. So the *first* transition out of a node is held strongly
 * instead, in fields on this object, with no `WeakRef` at all.
 *
 * That is safe because strong first-children form a path, not a tree: each node
 * pins at most one child, which pins at most one of its own. The retained set
 * per root is bounded by the depth of a builder chain — a handful of queries —
 * rather than by how many distinct queries an app builds. Everything past the
 * first child is weak, so the unbounded cases (a search box typing into
 * `where:title:=:s<text>`, a row id per list item) stay collectible.
 *
 * It is also where the wins are: a chain like `q.where(…).orderBy(…).limit(…)`
 * gives every node exactly one child, so building it allocates no `WeakRef`s and
 * looks up through a field compare rather than a hash lookup.
 *
 * What a collected child leaves behind is a cleared `WeakRef` and its map key. A
 * lookup drops those as it walks, which covers every key that is asked for
 * again. A key that is *never* asked for again is handled by the sweep.
 *
 * ## Best effort
 *
 * Interning is a cache, not a guarantee: a missed lookup only costs an extra
 * object. See {@linkcode MAX_SCAN} for the one case where we deliberately give
 * up.
 *
 * ## Identity vs. hash
 *
 * Identity implies an equal query hash, but the tree alone does not give the
 * converse. `normalizeAST` sorts `related` and `and`/`or` conditions, so
 * `q.where(a).where(b)` and `q.where(b).where(a)` hash the same while being
 * distinct paths, and so distinct nodes. {@linkcode HashIndex} is the second
 * level that folds those together once they have been hashed. Callers that
 * need the normalizing behavior for a query that may not have been must keep
 * using the hash.
 */

/**
 * The delta identifying a transition, or `undefined` when the key already
 * determines the result on its own.
 */
export type Delta = ReadonlyJSONValue | undefined;

type Entry<T extends object> = {
  readonly delta: Delta;
  readonly ref: WeakRef<T>;
};

/**
 * A bucket holds a bare entry in the common single-entry case and is promoted to
 * a `Set` only on collision, so the usual transition allocates no container.
 *
 * A `Set` rather than an array: deleting during iteration is well defined, so a
 * lookup prunes collected entries and stops at its match in a single pass, and
 * removing an entry is O(1) rather than a scan.
 */
type Bucket<T extends object> = Entry<T> | Set<Entry<T>>;

/** The varying argument of a transition, when it is one. */
export type TransitionValue = string | number | boolean | null | undefined;

/**
 * The longest `deepEqual` scan we are willing to do on a lookup.
 *
 * This is a *latency* bound, not a memory bound — memory is already handled,
 * since entries are weak and are pruned both by the sweep and by any lookup that
 * walks past them, so a bucket only ever holds children that are still alive.
 *
 * Note what happens at the limit: we stop *storing*, we do not evict. Evicting
 * would de-intern objects that are still in use, and on a sequential sweep of a
 * live set larger than the limit it drives the hit rate to zero — each lookup
 * throws out the entry the next one needs. Refusing to store instead leaves
 * every already-shared sibling shared and degrades the surplus to the old
 * behavior of a fresh instance per call.
 *
 * Buckets should rarely get near this. Keys carry the discriminating value
 * wherever it is cheap to, and a key that carries it is *exact*, so it costs no
 * comparison at all: every simple comparison goes that way, and so does a
 * one-hop `exists`, via its sub-query's identity. What is left sharing a bucket
 * is two-hop `exists`, the `and`/`or` trees an expression factory builds, and
 * relationships whose callback varies.
 */
export const MAX_SCAN = 64;

/** How large the weak map must get before its first sweep. */
const SWEEP_INITIAL = 64;

/** The transitions out of one node. */
export class Transitions<T extends object> {
  // The first transition, held strongly and in place. See "Liveness" above.
  #firstKey: string | undefined;
  #firstValue: TransitionValue;
  #firstDelta: Delta;
  #first: T | undefined;

  // Transitions whose key space is finite and comes from the schema rather than
  // from user data, held strongly. See `storeBounded`.
  #bounded: Map<string, T> | undefined;

  // Everything else, weakly. Allocated only if there is a second. Nested so the
  // value stays a `Map` key of its own rather than part of the string.
  #rest: Map<string, Map<TransitionValue, Bucket<T>>> | undefined;
  #sweepAt = SWEEP_INITIAL;
  #restSize = 0;

  /** The strongly held first child. Exposed for tests. */
  get first(): T | undefined {
    return this.#first;
  }

  /** The weakly held remainder. Exposed for tests. */
  get rest(): Map<string, Map<TransitionValue, Bucket<T>>> | undefined {
    return this.#rest;
  }

  /** Total weak entries across all keys. Exposed for tests. */
  get restSize(): number {
    return this.#restSize;
  }

  lookupBounded(key: string): T | undefined {
    return this.#bounded?.get(key);
  }

  /**
   * Records a transition whose key space is finite and schema-derived — the base
   * sub-query handed to a `related`/`exists` callback, keyed by relationship
   * name.
   *
   * These are held strongly. Retention is bounded by the number of relationships
   * on the table, so it cannot grow with what an app queries for, and in exchange
   * a relationship traversal allocates no `WeakRef`. Anything keyed by a *value*
   * — a row id, a search string, a limit — must not come through here.
   */
  storeBounded(key: string, value: T): void {
    (this.#bounded ??= new Map()).set(key, value);
  }

  lookup(key: string, value: TransitionValue, delta: Delta): T | undefined {
    if (
      this.#firstKey === key &&
      this.#firstValue === value &&
      deepEqual(this.#firstDelta, delta)
    ) {
      return this.#first;
    }
    const byValue = this.#rest?.get(key);
    if (byValue === undefined) {
      return undefined;
    }
    const bucket = byValue.get(value);
    if (bucket === undefined) {
      return undefined;
    }

    if (!(bucket instanceof Set)) {
      const found = bucket.ref.deref();
      if (found === undefined) {
        byValue.delete(value);
        this.#restSize--;
        return undefined;
      }
      // deepEqual short-circuits on reference equality, which is the fast path
      // we rely on for deltas built out of already-interned sub-queries.
      return deepEqual(bucket.delta, delta) ? found : undefined;
    }

    // One pass: drop what has been collected, stop at the match. Deleting from a
    // Set while iterating it is well defined — the iterator simply skips what is
    // gone — so this stays O(n) with no compaction step, and returns early on a
    // hit rather than walking the whole bucket first.
    for (const entry of bucket) {
      const found = entry.ref.deref();
      if (found === undefined) {
        bucket.delete(entry);
        this.#restSize--;
        continue;
      }
      if (deepEqual(entry.delta, delta)) {
        return found;
      }
    }
    if (bucket.size === 0) {
      byValue.delete(value);
    }
    return undefined;
  }

  store(key: string, value: TransitionValue, delta: Delta, node: T): void {
    if (this.#firstKey === undefined) {
      this.#firstKey = key;
      this.#firstValue = value;
      this.#firstDelta = delta;
      this.#first = node;
      return;
    }

    const rest = (this.#rest ??= new Map());
    let byValue = rest.get(key);
    if (byValue === undefined) {
      byValue = new Map();
      rest.set(key, byValue);
    }

    const entry: Entry<T> = {delta, ref: new WeakRef(node)};
    const bucket = byValue.get(value);

    if (bucket === undefined) {
      byValue.set(value, entry);
    } else if (!(bucket instanceof Set)) {
      byValue.set(value, new Set([bucket, entry]));
    } else {
      if (bucket.size >= MAX_SCAN) {
        // Leave the bucket alone; see MAX_SCAN. `node` is simply not shared.
        // A store only follows a failed lookup, which has just pruned, so this
        // is measured against entries that are actually still alive.
        return;
      }
      bucket.add(entry);
    }
    this.#restSize++;
    this.#maybeSweep(rest);
  }

  /**
   * Re-points the transition that produced `from` at `to`, so the next lookup
   * along that path yields `to` instead. This is how {@linkcode HashIndex}
   * converges two paths on one node: `from` keeps its identity, since it has
   * already been handed out, and its path is redirected for the rebuilds that
   * follow.
   *
   * Scans rather than taking a key. This runs once per redirected path, so a
   * walk here is cheaper than the fields every node would need to carry to
   * name its own transition.
   */
  replace(from: T, to: T): boolean {
    if (this.#first === from) {
      this.#first = to;
      return true;
    }
    const rest = this.#rest;
    if (rest === undefined) {
      return false;
    }
    for (const byValue of rest.values()) {
      for (const [value, bucket] of byValue) {
        if (!(bucket instanceof Set)) {
          if (bucket.ref.deref() === from) {
            byValue.set(value, {delta: bucket.delta, ref: new WeakRef(to)});
            return true;
          }
          continue;
        }
        for (const entry of bucket) {
          if (entry.ref.deref() === from) {
            bucket.delete(entry);
            bucket.add({delta: entry.delta, ref: new WeakRef(to)});
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Drops entries for collected values, if the map has grown enough to be worth
   * walking.
   *
   * A lookup already prunes whatever it walks past, which covers every key that
   * is asked for again. This is for the key that is not: a miss on a fresh key
   * returns without touching any existing entry, so nothing would ever notice
   * that those had died. Doubling the threshold off whatever survives keeps this
   * amortized O(1) per insert whether or not there was anything to reclaim, and
   * it is self-limiting — only a node that keeps taking inserts can keep
   * accumulating, and taking inserts is what triggers the sweep.
   */
  #maybeSweep(rest: Map<string, Map<TransitionValue, Bucket<T>>>): void {
    if (this.#restSize < this.#sweepAt) {
      return;
    }
    let live = 0;
    for (const [key, byValue] of rest) {
      for (const [value, bucket] of byValue) {
        if (!(bucket instanceof Set)) {
          if (bucket.ref.deref() === undefined) {
            byValue.delete(value);
          } else {
            live++;
          }
          continue;
        }
        for (const entry of bucket) {
          if (entry.ref.deref() === undefined) {
            bucket.delete(entry);
          } else {
            live++;
          }
        }
        if (bucket.size === 0) {
          byValue.delete(value);
        }
      }
      if (byValue.size === 0) {
        rest.delete(key);
      }
    }
    this.#restSize = live;
    this.#sweepAt = Math.max(SWEEP_INITIAL, live * 2);
  }
}

/**
 * The second level of interning: nodes keyed by *content* rather than by the
 * path that built them.
 *
 * The transition tree cannot see that `q.where(a).where(b)` and
 * `q.where(b).where(a)` are the same query. `normalizeAST` sorts conditions
 * and `related` entries, so the two build byte-identical ASTs, but they are
 * different paths and so different nodes. This index closes that gap, keyed by
 * the query's hash.
 *
 * It is consulted when a query is *hashed*, not when it is built. Hashing on
 * every tree miss would make a cold chain pay for a hash per node, where the
 * uninterned code paid for one at the end, if at all -- and server-side queries
 * are built cold, once, per request. The consumers that key by hash already
 * hash the queries they care about, so doing the work there costs one map
 * lookup on top of a hash that was being computed anyway, and nothing at all
 * for a query nobody hashes.
 *
 * On a hit the query being hashed keeps its identity, since it has already
 * been handed out. Its *transition* is re-pointed at the existing node instead
 * (see {@linkcode Transitions.replace}), so the next rebuild along that path
 * yields the shared instance. As with the tree this is a cache: the rebuild
 * after a redirect is what converges, not the call that found the duplicate.
 *
 * One index per root, which is what scopes it. Two roots never share an index,
 * so queries against different schemas or different delegates -- neither of
 * which the hash covers -- can never be conflated. Entries are weak: a cleared
 * one is dropped by the lookup that finds it, or by a sweep once the map has
 * doubled since the last one.
 */
export class HashIndex<T extends object> {
  readonly #byHash = new Map<string, WeakRef<T>>();
  #sweepAt = SWEEP_INITIAL;

  /** The underlying map. Exposed for tests. */
  get map(): Map<string, WeakRef<T>> {
    return this.#byHash;
  }

  get(hash: string): T | undefined {
    const ref = this.#byHash.get(hash);
    if (ref === undefined) {
      return undefined;
    }
    const node = ref.deref();
    if (node === undefined) {
      this.#byHash.delete(hash);
    }
    return node;
  }

  set(hash: string, node: T): void {
    const byHash = this.#byHash;
    byHash.set(hash, new WeakRef(node));
    if (byHash.size < this.#sweepAt) {
      return;
    }
    for (const [h, ref] of byHash) {
      if (ref.deref() === undefined) {
        byHash.delete(h);
      }
    }
    this.#sweepAt = Math.max(SWEEP_INITIAL, byHash.size * 2);
  }
}
