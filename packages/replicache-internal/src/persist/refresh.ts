import type * as dag from '../dag/mod.js';
import * as db from '../db/mod.js';
import * as sync from '../sync/mod.js';
import {
  assertClientDD31,
  ClientStateNotFoundError,
  getClient,
  getClientGroupForClient,
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

  // Suspend eviction and deletion of chunks cached by the lazy store
  // to prevent cache misses.  If eviction and deletion are not suspended
  // some chunks that are not gathered due to already being cached, may be
  // evicted or deleted by the time the write lock is acquired on the memdag,
  // which can lead to cache misses when performing the rebase and diff.
  // It is important to avoid these cache misses because they often create jank
  // because they block local mutations, pulls and queries on reading from idb.
  // Cache misses can still happen during the rebase and diff, but only
  // if the gather step hits its size limit.
  const result:
    | [
        newMemdagHeadHash: Hash,
        diffs: sync.DiffsMap,
        newPerdagClientHeadHash: Hash,
      ]
    | undefined = await memdag.withSuspendedSourceCacheEvictsAndDeletes(
    async () => {
      const perdagWriteResult:
        | [
            Hash,
            db.Commit<db.SnapshotMetaDD31>,
            number,
            ReadonlyMap<Hash, ChunkWithSize>,
          ]
        | undefined = await perdag.withWrite(async perdagWrite => {
        const clientGroup = await getClientGroupForClient(
          clientID,
          perdagWrite,
        );
        if (!clientGroup) {
          throw new ClientStateNotFoundError(clientID);
        }

        const perdagClientGroupHeadHash = clientGroup.headHash;
        const perdagClientGroupHeadCommit = await db.commitFromHash(
          perdagClientGroupHeadHash,
          perdagWrite,
        );
        const perdagLmid = await perdagClientGroupHeadCommit.getMutationID(
          clientID,
          perdagWrite,
        );

        // Need to pull this head into memdag, but can't have it disappear if
        // perdag moves forward while we're rebasing in memdag. Can't change
        // client headHash until our rebase in memdag is complete, because if
        // rebase fails, then nothing is keeping client's chunks alive in
        // perdag.
        const client = await getClient(clientID, perdagWrite);
        if (!client) {
          throw new ClientStateNotFoundError(clientID);
        }
        assertClientDD31(client);
        const perdagClientGroupBaseSnapshot = await db.baseSnapshotFromHash(
          perdagClientGroupHeadHash,
          perdagWrite,
        );
        assertSnapshotCommitDD31(perdagClientGroupBaseSnapshot);
        if (
          shouldAbortRefresh(
            memdagBaseSnapshot,
            perdagClientGroupBaseSnapshot,
            perdagClientGroupHeadHash,
          )
        ) {
          return undefined;
        }

        // To avoid pulling the entire perdag graph into the memdag
        // the amount of chunk data gathered is limited by size.
        const visitor = new GatherNotCachedVisitor(
          perdagWrite,
          memdag,
          GATHER_SIZE_LIMIT,
        );
        await visitor.visitCommit(perdagClientGroupHeadHash);
        const {gatheredChunks} = visitor;

        const newClient = {
          ...client,
          tempRefreshHash: perdagClientGroupHeadHash,
        };
        await setClient(clientID, newClient, perdagWrite);
        await perdagWrite.commit();
        return [
          perdagClientGroupHeadHash,
          perdagClientGroupBaseSnapshot,
          perdagLmid,
          gatheredChunks,
        ];
      });

      if (closed() || !perdagWriteResult) {
        return;
      }

      const [
        perdagClientGroupHeadHash,
        perdagClientGroupBaseSnapshot,
        perdagLmid,
        gatheredChunks,
      ] = perdagWriteResult;
      return await memdag.withWrite(async memdagWrite => {
        const memdagHeadCommit = await db.commitFromHead(
          db.DEFAULT_HEAD_NAME,
          memdagWrite,
        );
        const memdagBaseSnapshot = await db.baseSnapshotFromCommit(
          memdagHeadCommit,
          memdagWrite,
        );
        if (
          shouldAbortRefresh(
            memdagBaseSnapshot,
            perdagClientGroupBaseSnapshot,
            perdagClientGroupHeadHash,
          )
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

        let newMemdagHeadHash = perdagClientGroupHeadHash;
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
        return [newMemdagHeadHash, diffs, perdagClientGroupHeadHash];
      });
    },
  );

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
      headHash: result === undefined ? client.headHash : result[2],
      tempRefreshHash: null,
    };

    // If this cleanup never happens, it's no big deal, some data will stay
    // alive longer but next refresh will fix it.
    await setClient(clientID, newClient, perdagWrite);
  });

  return result && [result[0], result[1]];
}

function shouldAbortRefresh(
  memdagBaseSnapshot: db.Commit<db.SnapshotMeta | db.SnapshotMetaDD31>,
  perdagClientGroupBaseSnapshot: db.Commit<
    db.SnapshotMeta | db.SnapshotMetaDD31
  >,
  perdagClientGroupHeadHash: Hash,
): boolean {
  const baseSnapshotCookieCompareResult = db.compareCookiesForSnapshots(
    memdagBaseSnapshot,
    perdagClientGroupBaseSnapshot,
  );
  return (
    baseSnapshotCookieCompareResult > 0 ||
    (baseSnapshotCookieCompareResult === 0 &&
      perdagClientGroupHeadHash === perdagClientGroupBaseSnapshot.chunk.hash)
  );
}
