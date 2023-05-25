import type {LogContext} from '@rocicorp/logger';
import {sleep} from 'shared/sleep.js';
import type * as dag from '../dag/mod.js';
import {assertSnapshotCommitDD31} from '../db/commit.js';
import * as db from '../db/mod.js';
import type {FormatVersion} from '../format-version.js';
import type {Hash} from '../hash.js';
import type {MutatorDefs} from '../replicache.js';
import type {ClientID} from '../sync/ids.js';
import * as sync from '../sync/mod.js';
import {withRead, withWrite} from '../with-transactions.js';
import {
  ClientStateNotFoundError,
  ClientV6,
  assertClientV6,
  getClientGroupForClient,
  mustGetClient,
  setClient,
} from './clients.js';
import {
  ChunkWithSize,
  GatherNotCachedVisitor,
} from './gather-not-cached-visitor.js';

const GATHER_SIZE_LIMIT = 5 * 2 ** 20; // 5 MB
const DELAY_MS = 300;

type RefreshResult =
  | {
      type: 'aborted';
      refreshHashesForRevert?: readonly Hash[] | undefined;
    }
  | {
      type: 'complete';
      newMemdagHeadHash: Hash;
      diffs: sync.DiffsMap;
      newPerdagClientHeadHash: Hash;
    };

/**
 * This returns the diff between the state of the btree before and after
 * refresh. It returns `undefined` if the refresh was aborted.
 */
export async function refresh(
  lc: LogContext,
  memdag: dag.LazyStore,
  perdag: dag.Store,
  clientID: ClientID,
  mutators: MutatorDefs,
  diffConfig: sync.DiffComputationConfig,
  closed: () => boolean,
  formatVersion: FormatVersion,
): Promise<[Hash, sync.DiffsMap] | undefined> {
  if (closed()) {
    return;
  }
  const memdagBaseSnapshot = await withRead(memdag, memdagRead =>
    db.baseSnapshotFromHead(db.DEFAULT_HEAD_NAME, memdagRead),
  );
  assertSnapshotCommitDD31(memdagBaseSnapshot);

  type PerdagWriteResult = [
    perdagClientGroupHeadHash: Hash,
    perdagClientGroupBaseSnapshot: db.Commit<db.SnapshotMetaDD31>,
    perdagLmid: number,
    gatheredChunks: ReadonlyMap<Hash, ChunkWithSize>,
    refreshHashesForRevert: readonly Hash[],
  ];

  // Suspend eviction and deletion of chunks cached by the lazy store
  // to prevent cache misses.  If eviction and deletion are not suspended
  // some chunks that are not gathered due to already being cached, may be
  // evicted or deleted by the time the write lock is acquired on the memdag,
  // which can lead to cache misses when performing the rebase and diff.
  // It is important to avoid these cache misses because they often create jank
  // because they block local mutations, pulls and queries on reading from idb.
  // Cache misses can still happen during the rebase and diff, but only
  // if the gather step hits its size limit.
  const result: RefreshResult =
    await memdag.withSuspendedSourceCacheEvictsAndDeletes(async () => {
      const perdagWriteResult: PerdagWriteResult | undefined = await withWrite(
        perdag,
        async perdagWrite => {
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
          const client = await mustGetClient(clientID, perdagWrite);
          assertClientV6(client);
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
          await visitor.visit(perdagClientGroupHeadHash);
          const {gatheredChunks} = visitor;

          const refreshHashesSet = new Set(client.refreshHashes);
          refreshHashesSet.add(perdagClientGroupHeadHash);

          const newClient: ClientV6 = {
            ...client,
            refreshHashes: [...refreshHashesSet],
          };

          await setClient(clientID, newClient, perdagWrite);
          await perdagWrite.commit();
          return [
            perdagClientGroupHeadHash,
            perdagClientGroupBaseSnapshot,
            perdagLmid,
            gatheredChunks,
            client.refreshHashes,
          ];
        },
      );

      if (closed() || !perdagWriteResult) {
        return {
          type: 'aborted',
        };
      }
      // pull/poke and refresh are racing to see who gets to update
      // the memdag (the one with the newer base snapshot cookie wins)
      // pull/poke updates are preferable so delay refresh slightly to
      // make pull/poke the winner except when pull/pokes are slow.
      // This is especially important for pokes, as refresh winning
      // will result in the next poke's cookie not matching necessitating
      // a disconnect/reconnect.
      await sleep(DELAY_MS);
      if (closed()) {
        return {
          type: 'aborted',
        };
      }

      const [
        perdagClientGroupHeadHash,
        perdagClientGroupBaseSnapshot,
        perdagLmid,
        gatheredChunks,
        refreshHashesForRevert,
      ] = perdagWriteResult;
      return withWrite(memdag, async memdagWrite => {
        const memdagHeadCommit = await db.commitFromHead(
          db.DEFAULT_HEAD_NAME,
          memdagWrite,
        );
        const memdagBaseSnapshot = await db.baseSnapshotFromCommit(
          memdagHeadCommit,
          memdagWrite,
        );
        assertSnapshotCommitDD31(memdagBaseSnapshot);
        if (
          shouldAbortRefresh(
            memdagBaseSnapshot,
            perdagClientGroupBaseSnapshot,
            perdagClientGroupHeadHash,
          )
        ) {
          return {
            type: 'aborted',
            refreshHashesForRevert,
          };
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
              newMemdagMutations[i].meta.clientID,
              formatVersion,
            )
          ).chunk.hash;
        }

        const diffs = await sync.diffCommits(
          memdagHeadCommit,
          await db.commitFromHash(newMemdagHeadHash, memdagWrite),
          memdagWrite,
          diffConfig,
          formatVersion,
        );

        await memdagWrite.setHead(db.DEFAULT_HEAD_NAME, newMemdagHeadHash);
        await memdagWrite.commit();
        return {
          type: 'complete',
          newMemdagHeadHash,
          diffs,
          newPerdagClientHeadHash: perdagClientGroupHeadHash,
        };
      });
    });

  if (closed()) {
    return;
  }

  const setRefreshHashes = (refreshHashes: readonly Hash[]) =>
    withWrite(perdag, async perdagWrite => {
      const client = await mustGetClient(clientID, perdagWrite);
      const newClient = {
        ...client,
        refreshHashes,
      };

      // If this cleanup never happens, it's no big deal, some data will stay
      // alive longer but next refresh will fix it.
      await setClient(clientID, newClient, perdagWrite);

      await perdagWrite.commit();
    });

  if (result.type === 'aborted') {
    if (result.refreshHashesForRevert) {
      await setRefreshHashes(result.refreshHashesForRevert);
    }
    return undefined;
  }
  await setRefreshHashes([result.newPerdagClientHeadHash]);
  return [result.newMemdagHeadHash, result.diffs];
}

function shouldAbortRefresh(
  memdagBaseSnapshot: db.Commit<db.SnapshotMetaDD31>,
  perdagClientGroupBaseSnapshot: db.Commit<db.SnapshotMetaDD31>,
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
