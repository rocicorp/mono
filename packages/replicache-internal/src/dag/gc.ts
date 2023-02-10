import {assertNumber} from '../asserts';
import type {Hash} from '../hash';

export type HeadChange = {
  new: Hash | undefined;
  old: Hash | undefined;
};

export type RefCountUpdates = Map<Hash, number>;
type LoadedRefCountPromises = Map<Hash, Promise<number>>;

export interface GarbageCollectionDelegate {
  getRefCount: (hash: Hash) => Promise<number | undefined>;
  getRefs: (hash: Hash) => Promise<readonly Hash[] | undefined>;
}

/**
 * Computes how ref counts should be updated when a dag write is committed.
 * Does not modify the dag store.
 * @param headChanges Heads that were changed by the dag write.
 * @param putChunks Chunks that were put by the dag write.
 * @param delegate Delegate used for loading ref information from the dag store.
 * @returns Map from chunk Hash to new ref count.  Chunks with a new ref count of 0 should
 * be deleted.  All hashes in `putChunks` will have an entry (which will be zero if the
 * newly put chunk is not reachable from any head).
 */
export async function computeRefCountUpdates(
  headChanges: Iterable<HeadChange>,
  putChunks: ReadonlySet<Hash>,
  delegate: GarbageCollectionDelegate,
  newCacheReadonly?: ReadonlySet<Hash>,
): Promise<RefCountUpdates> {
  const newHeads: Hash[] = [];
  const oldHeads: Hash[] = [];
  for (const changedHead of headChanges) {
    if (changedHead.old !== changedHead.new) {
      changedHead.old && oldHeads.push(changedHead.old);
      changedHead.new && newHeads.push(changedHead.new);
    }
  }

  const refCountUpdates: RefCountUpdates = new Map();
  // This map is used to ensure we do not load the ref count key more than once.
  // Once it is loaded we only operate on a cache of the ref counts.
  const loadedRefCountPromises: LoadedRefCountPromises = new Map();

  const newCache = new Set(newCacheReadonly);
  for (const n of newHeads) {
    await changeRefCount(
      n,
      1,
      refCountUpdates,
      loadedRefCountPromises,
      delegate,
      newCache,
    );
  }

  await Promise.all(
    Array.from(newCache.values(), hash =>
      ensureRefCountLoaded(
        hash,
        refCountUpdates,
        loadedRefCountPromises,
        delegate,
      ),
    ),
  );

  let refCountUpdated;
  do {
    refCountUpdated = false;
    for (const hash of newCache.values()) {
      if (refCountUpdates.get(hash) !== 0) {
        newCache.delete(hash);
        const refs = await delegate.getRefs(hash);
        if (refs !== undefined) {
          const ps = refs.map(ref =>
            changeRefCount(
              ref,
              1,
              refCountUpdates,
              loadedRefCountPromises,
              delegate,
              newCache,
            ),
          );
          await Promise.all(ps);
          refCountUpdated = true;
          break;
        }
      }
    }
  } while (refCountUpdated);

  for (const o of oldHeads) {
    await changeRefCount(
      o,
      -1,
      refCountUpdates,
      loadedRefCountPromises,
      delegate,
    );
  }

  // Now go through the put chunks to ensure each has an entry in refCountUpdates
  // (zero for new chunks which are not reachable from newHeads).
  await Promise.all(
    Array.from(putChunks.values(), hash =>
      ensureRefCountLoaded(
        hash,
        refCountUpdates,
        loadedRefCountPromises,
        delegate,
      ),
    ),
  );

  return refCountUpdates;
}

async function changeRefCount(
  hash: Hash,
  delta: number,
  refCountUpdates: RefCountUpdates,
  loadedRefCountPromises: LoadedRefCountPromises,
  delegate: GarbageCollectionDelegate,
  newCache?: Set<Hash>,
): Promise<void> {
  // First make sure that we have the ref count in the cache. This is async
  // because it might need to load the ref count from the store (via the delegate).
  //
  // Once we have loaded the ref count all the updates to it are sync to
  // prevent race conditions.
  await ensureRefCountLoaded(
    hash,
    refCountUpdates,
    loadedRefCountPromises,
    delegate,
  );

  if (updateRefCount(hash, delta, refCountUpdates)) {
    newCache?.delete(hash);
    const refs = await delegate.getRefs(hash);
    if (refs !== undefined) {
      const ps = refs.map(ref =>
        changeRefCount(
          ref,
          delta,
          refCountUpdates,
          loadedRefCountPromises,
          delegate,
          newCache,
        ),
      );
      await Promise.all(ps);
    }
  }
}

function ensureRefCountLoaded(
  hash: Hash,
  refCountUpdates: RefCountUpdates,
  loadedRefCountPromises: LoadedRefCountPromises,
  delegate: GarbageCollectionDelegate,
): Promise<number> {
  // Only get the ref count once.
  let p = loadedRefCountPromises.get(hash);
  if (p === undefined) {
    p = (async () => {
      const value = (await delegate.getRefCount(hash)) || 0;
      refCountUpdates.set(hash, value);
      return value;
    })();
    loadedRefCountPromises.set(hash, p);
  }
  return p;
}

function updateRefCount(
  hash: Hash,
  delta: number,
  refCountUpdates: RefCountUpdates,
): boolean {
  const oldCount = refCountUpdates.get(hash);
  assertNumber(oldCount);
  refCountUpdates.set(hash, oldCount + delta);
  return (oldCount === 0 && delta === 1) || (oldCount === 1 && delta === -1);
}
