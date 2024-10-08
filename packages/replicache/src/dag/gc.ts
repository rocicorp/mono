import {assert, assertNumber} from '../../../shared/src/asserts.js';
import type {MaybePromise} from '../../../shared/src/types.js';
import {skipGCAsserts} from '../config.js';
import {type Hash, emptyHash} from '../hash.js';

export type HeadChange = {
  new: Hash | undefined;
  old: Hash | undefined;
};

type LoadedRefCountPromises = Map<Hash, Promise<number>>;

export interface RefCountUpdatesDelegate {
  getRefCount: (hash: Hash) => MaybePromise<number | undefined>;
  getRefs: (hash: Hash) => MaybePromise<readonly Hash[] | undefined>;
  /**
   * Should be implemented if the store lazily loads refs, returning whether
   * or not the chunks refs have already been counted (i.e. are reflected
   * in `getRefCount`).
   *
   * If defined then:
   *  - `getRefs` should return undefined for refs that have not been loaded,
   *    but should never return undefined for hashes in `putChunks`.
   *  - it is assumed that chunks in `putChunks` may have been reachable before
   *    the write, but may not have been counted.  This method is used to
   *    determine if they have been counted or not.  If they have not been
   *    counted, and are reachable with the write applied, the returned
   *    ref count updates will include updates for counting them.
   *
   * If undefined then:
   *  - `getRefs` should never return undefined
   *  - it is assumed that the refs of any chunks which were reachable before
   *    the write are already counted
   */
  areRefsCounted?: (hash: Hash) => boolean;
}

/**
 * Computes how ref counts should be updated when a dag write is committed.
 * Does not modify the dag store.
 * @param headChanges Heads that were changed by the dag write.
 * @param putChunks Chunks that were put by the dag write.
 * @param delegate Delegate used for getting ref information from the dag store.
 * @returns Map from chunk Hash to changed ref counts.  Chunks with a new ref
 * count of 0 should be deleted.  All hashes in `putChunks` will have an entry
 * (which will be zero if the newly put chunk is not reachable from any head).
 */
export function computeRefCountUpdates(
  headChanges: Iterable<HeadChange>,
  putChunks: ReadonlySet<Hash>,
  delegate: RefCountUpdatesDelegate,
): Promise<Map<Hash, number>> {
  return new RefCountUpdates(headChanges, putChunks, delegate).compute();
}

class RefCountUpdates {
  readonly #newHeads: Hash[];
  readonly #oldHeads: Hash[];
  readonly #putChunks: ReadonlySet<Hash>;
  readonly #delegate: RefCountUpdatesDelegate;
  readonly #refsCounted: Set<Hash> | null;
  readonly #refCountUpdates: Map<Hash, number>;
  readonly #loadedRefCountPromises: LoadedRefCountPromises;
  readonly #isLazyDelegate: boolean;

  constructor(
    headChanges: Iterable<HeadChange>,
    putChunks: ReadonlySet<Hash>,
    delegate: RefCountUpdatesDelegate,
  ) {
    const newHeads: Hash[] = [];
    const oldHeads: Hash[] = [];
    for (const changedHead of headChanges) {
      if (changedHead.old !== changedHead.new) {
        changedHead.old && oldHeads.push(changedHead.old);
        changedHead.new && newHeads.push(changedHead.new);
      }
    }
    this.#newHeads = newHeads;
    this.#oldHeads = oldHeads;
    this.#putChunks = putChunks;
    this.#delegate = delegate;
    this.#refCountUpdates = new Map();
    // This map is used to ensure we do not load the ref count key more than once.
    // Once it is loaded we only operate on a cache of the ref counts.
    this.#loadedRefCountPromises = new Map();
    this.#isLazyDelegate = delegate.areRefsCounted !== undefined;
    this.#refsCounted = this.#isLazyDelegate ? new Set() : null;
  }

  async compute(): Promise<Map<Hash, number>> {
    for (const n of this.#newHeads) {
      await this.#changeRefCount(n, 1);
    }

    // Now go through the put chunks to ensure each has an entry in
    // refCountUpdates (zero for new chunks which are not reachable from
    // newHeads).
    await Promise.all(
      Array.from(this.#putChunks.values(), hash =>
        this.#ensureRefCountLoaded(hash),
      ),
    );

    if (this.#isLazyDelegate) {
      assert(this.#delegate.areRefsCounted);
      assert(this.#refsCounted);
      let refCountsUpdated;
      do {
        refCountsUpdated = false;
        for (const hash of this.#putChunks.values()) {
          if (
            !this.#delegate.areRefsCounted(hash) &&
            !this.#refsCounted.has(hash) &&
            this.#refCountUpdates.get(hash) !== 0
          ) {
            await this.#updateRefsCounts(hash, 1);
            refCountsUpdated = true;
            break;
          }
        }
      } while (refCountsUpdated);
    }

    for (const o of this.#oldHeads) {
      await this.#changeRefCount(o, -1);
    }

    if (!skipGCAsserts) {
      for (const [hash, update] of this.#refCountUpdates) {
        assert(
          update >= 0,
          `ref count update must be non-negative. ${hash}:${update}`,
        );
      }
    }

    return this.#refCountUpdates;
  }

  async #changeRefCount(hash: Hash, delta: number): Promise<void> {
    // First make sure that we have the ref count in the cache. This is async
    // because it might need to load the ref count from the store (via the delegate).
    //
    // Once we have loaded the ref count all the updates to it are sync to
    // prevent race conditions.
    await this.#ensureRefCountLoaded(hash);
    if (this.#updateRefCount(hash, delta)) {
      await this.#updateRefsCounts(hash, delta);
    }
  }

  async #updateRefsCounts(hash: Hash, delta: number) {
    if (hash === emptyHash) {
      return;
    }
    const refs = await this.#delegate.getRefs(hash);
    if (!skipGCAsserts) {
      assert(
        refs || (this.#isLazyDelegate && !this.#putChunks.has(hash)),
        'refs must be defined',
      );
    }

    if (refs !== undefined) {
      this.#refsCounted?.add(hash);
      const ps = refs.map(ref => this.#changeRefCount(ref, delta));
      await Promise.all(ps);
    }
  }

  #ensureRefCountLoaded(hash: Hash): Promise<number> {
    // Only get the ref count once.
    let p = this.#loadedRefCountPromises.get(hash);
    if (p === undefined) {
      p = (async () => {
        const value = (await this.#delegate.getRefCount(hash)) || 0;
        this.#refCountUpdates.set(hash, value);
        return value;
      })();
      this.#loadedRefCountPromises.set(hash, p);
    }
    return p;
  }

  #updateRefCount(hash: Hash, delta: number): boolean {
    const oldCount = this.#refCountUpdates.get(hash);
    assertNumber(oldCount);
    this.#refCountUpdates.set(hash, oldCount + delta);
    return (oldCount === 0 && delta === 1) || (oldCount === 1 && delta === -1);
  }
}
