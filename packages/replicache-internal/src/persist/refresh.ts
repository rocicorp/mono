import type * as dag from '../dag/mod.js';
import * as db from '../db/mod.js';
import * as sync from '../sync/mod.js';
import {
  assertClientDD31,
  ClientStateNotFoundError,
  getClient,
  getMainClientGroup,
  setClient,
} from './clients.js';
import type {MutatorDefs} from '../replicache.js';
import type {Hash} from '../hash.js';
import type {LogContext} from '@rocicorp/logger';
import {assertSnapshotCommitDD31} from '../db/commit.js';
import {
  ChunkWithSize,
  GatherNotCachedVisitor,
} from './gather-not-cached-visitor.js';

const GATHER_SIZE_LIMIT = 5 * 2 ** 20; // 5 MB

/**
 * This returns the diff between the state of the btree before and after
 * refresh. It returns `undefined` if the refresh was aborted.
 */
export async function refresh(
  lc: LogContext,
  memdag: dag.LazyStore,
  perdag: dag.Store,
  clientID: sync.ClientID,
  mutators: MutatorDefs,
  diffConfig: sync.DiffComputationConfig,
  closed: () => boolean,
): Promise<[Hash, sync.DiffsMap] | undefined> {
  if (closed()) {
    return;
  }
  const memdagBaseSnapshot = await memdag.withRead(memdagRead =>
    db.baseSnapshotFromHead(db.DEFAULT_HEAD_NAME, memdagRead),
  );

  const perdagWriteResult:
    | [
        Hash,
        db.Commit<db.SnapshotMetaDD31>,
        number,
        ReadonlyMap<Hash, ChunkWithSize>,
      ]
    | undefined = await perdag.withWrite(async perdagWrite => {
    const mainClientGroup = await getMainClientGroup(clientID, perdagWrite);
    if (!mainClientGroup) {
      throw new ClientStateNotFoundError(clientID);
    }

    const perdagMainHeadHash = mainClientGroup.headHash;
    const perdagMainHeadCommit = await db.commitFromHash(
      perdagMainHeadHash,
      perdagWrite,
    );
    const perdagLmid = await perdagMainHeadCommit.getMutationID(
      clientID,
      perdagWrite,
    );

    // Need to pull this head into memdag, but can't have it disappear if
    // perdag moves forward while we're rebasing in memdag. Can't change client
    // headHash until our rebase in memdag is complete, because if rebase fails,
    // then nothing is keeping client's main alive in perdag.
    const client = await getClient(clientID, perdagWrite);
    if (!client) {
      throw new ClientStateNotFoundError(clientID);
    }
    assertClientDD31(client);
    const perdagMainBaseSnapshot = await db.baseSnapshotFromHash(
      perdagMainHeadHash,
      perdagWrite,
    );
    assertSnapshotCommitDD31(perdagMainBaseSnapshot);
    if (
      db.compareCookiesForSnapshots(
        memdagBaseSnapshot,
        perdagMainBaseSnapshot,
      ) > 0
    ) {
      return undefined;
    }

    // To avoid pulling the entire perdag graph into the memdag
    // the amount of chunk data gathered is limited by size.
    //
    // Note that a write lock is not held on the memdag while not-cached chunks
    // are gathered.  This means that the `isCached` value for chunks can change
    // during the gather process due to cache population, cache eviction
    // and/or chunk GC in the memdag.  So when the write lock is acquired
    // on the memdag below gatheredChunks may contain some chunks that are
    // already cached and may be missing some chunks that are not-cached.  It
    // may also be missing chunks that are not-cached due to the gather size
    // limit.  This is OK, because the already cached chunks will
    // just be re-put into the memdag (a no-op), and the missing
    // not-cache chunks will just be cache misses that will then be
    // loaded from the perdag and put into the cache.
    // This gather and write approach aims to minimize cache misses during
    // the below rebase and diff steps.  This is important because
    // cache misses are relatively slow (as they require reading from idb),
    // and thus handling them while having the memdag locked often creates jank
    // by blocking local mutations, pulls and queries on idb.  This approach
    // does not eliminate all cache misses, but it does minimize them.
    const visitor = new GatherNotCachedVisitor(
      perdagWrite,
      memdag,
      GATHER_SIZE_LIMIT,
    );
    await visitor.visitCommit(perdagMainHeadHash);
    const {gatheredChunks} = visitor;

    const newClient = {
      ...client,
      tempRefreshHash: perdagMainHeadHash,
    };
    await setClient(clientID, newClient, perdagWrite);
    await perdagWrite.commit();
    return [
      perdagMainHeadHash,
      perdagMainBaseSnapshot,
      perdagLmid,
      gatheredChunks,
    ];
  });

  if (closed() || !perdagWriteResult) {
    return;
  }

  const [
    perdagMainHeadHash,
    perdagMainBaseSnapshot,
    perdagLmid,
    gatheredChunks,
  ] = perdagWriteResult;
  const memdagWriteResult: [Hash, sync.DiffsMap] | undefined =
    await memdag.withWrite(async memdagWrite => {
      const memdagHeadCommit = await db.commitFromHead(
        db.DEFAULT_HEAD_NAME,
        memdagWrite,
      );
      const memdagBaseSnapshot = await db.baseSnapshotFromCommit(
        memdagHeadCommit,
        memdagWrite,
      );
      if (
        db.compareCookiesForSnapshots(
          memdagBaseSnapshot,
          perdagMainBaseSnapshot,
        ) > 0
      ) {
        return undefined;
      }

      const newMemdagMutations = await db.localMutationsGreaterThan(
        memdagHeadCommit,
        {[clientID]: perdagLmid},
        memdagWrite,
      );
      const ps = [];
      for (const {chunk, size} of gatheredChunks.values()) {
        ps.push(memdagWrite.putChunk(chunk, size));
      }
      await Promise.all(ps);

      let newMemdagHeadHash = perdagMainHeadHash;
      for (let i = newMemdagMutations.length - 1; i >= 0; i--) {
        newMemdagHeadHash = (
          await db.rebaseMutationAndPutCommit(
            newMemdagMutations[i],
            memdagWrite,
            newMemdagHeadHash,
            mutators,
            lc,
            clientID,
          )
        ).chunk.hash;
      }

      const diffs = await sync.diffCommits(
        memdagHeadCommit,
        await db.commitFromHash(newMemdagHeadHash, memdagWrite),
        memdagWrite,
        diffConfig,
      );

      await memdagWrite.setHead(db.DEFAULT_HEAD_NAME, newMemdagHeadHash);
      await memdagWrite.commit();
      return [newMemdagHeadHash, diffs];
    });

  if (closed()) {
    return;
  }
  await perdag.withWrite(async perdagWrite => {
    const client = await getClient(clientID, perdagWrite);
    if (!client) {
      throw new ClientStateNotFoundError(clientID);
    }
    const newClient = {
      ...client,
      headHash:
        memdagWriteResult === undefined ? client.headHash : perdagMainHeadHash,
      tempRefreshHash: null,
    };

    // If this cleanup never happens, it's no big deal, some data will stay
    // alive longer but next refresh will fix it.
    await setClient(clientID, newClient, perdagWrite);
  });

  return memdagWriteResult;
}
