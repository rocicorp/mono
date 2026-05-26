import type {LogContext} from '@rocicorp/logger';
import type {Enum} from '../../../shared/src/enum.ts';
import type {LazyStore} from '../dag/lazy-store.ts';
import type {Store} from '../dag/store.ts';
import type {Commit} from '../db/commit.ts';
import {
  DEFAULT_HEAD_NAME,
  type SnapshotMetaDD31,
  assertSnapshotCommitDD31,
  baseSnapshotFromCommit,
  baseSnapshotFromHash,
  baseSnapshotFromHead,
  commitFromHash,
  commitFromHead,
  compareCookiesForSnapshots,
  localMutationsGreaterThan,
} from '../db/commit.ts';
import {rebaseMutationAndPutCommit} from '../db/rebase.ts';
import type * as FormatVersion from '../format-version-enum.ts';
import type {Hash} from '../hash.ts';
import type {ZeroOption} from '../replicache-options.ts';
import type {DiffsMap} from '../sync/diff.ts';
import {type DiffComputationConfig, diffCommits} from '../sync/diff.ts';
import type {ClientID} from '../sync/ids.ts';
import type {MutatorDefs} from '../types.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {
  ClientStateNotFoundError,
  type ClientV6,
  assertClientV6,
  getClientGroupForClient,
  mustGetClient,
  setClient,
} from './clients.ts';
import {
  type ChunkWithSize,
  GatherNotCachedVisitor,
} from './gather-not-cached-visitor.ts';

type FormatVersion = Enum<typeof FormatVersion>;

const GATHER_SIZE_LIMIT = 5 * 2 ** 20; // 5 MB

type RefreshResult =
  | {
      type: 'aborted';
      refreshHashesForRevert?: readonly Hash[] | undefined;
    }
  | {
      type: 'complete';
      diffs: DiffsMap;
      newPerdagClientHeadHash: Hash;
      oldHead: Hash;
      newHead: Hash;
    };

/**
 * This returns the diff between the state of the btree before and after
 * refresh. It returns `undefined` if the refresh was aborted.
 */
export async function refresh(
  lc: LogContext,
  memdag: LazyStore,
  perdag: Store,
  clientID: ClientID,
  mutators: MutatorDefs,
  diffConfig: DiffComputationConfig,
  closed: () => boolean,
  formatVersion: FormatVersion,
  zero: ZeroOption | undefined,
): Promise<{oldHead: Hash; newHead: Hash; diffs: DiffsMap} | undefined> {
  if (closed()) {
    return;
  }
  const memdagBaseSnapshot = await withRead(memdag, memdagRead =>
    baseSnapshotFromHead(DEFAULT_HEAD_NAME, memdagRead),
  );
  assertSnapshotCommitDD31(memdagBaseSnapshot);

  type PerdagWriteResult = [
    perdagClientGroupHeadHash: Hash,
    perdagClientGroupBaseSnapshot: Commit<SnapshotMetaDD31>,
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
          const perdagClientGroupHeadCommit = await commitFromHash(
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
          const perdagClientGroupBaseSnapshot = await baseSnapshotFromHash(
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
        } as const;
      }

      const [
        perdagClientGroupHeadHash,
        perdagClientGroupBaseSnapshot,
        perdagLmid,
        gatheredChunks,
        refreshHashesForRevert,
      ] = perdagWriteResult;
      return withWrite(memdag, async memdagWrite => {
        const memdagHeadCommit = await commitFromHead(
          DEFAULT_HEAD_NAME,
          memdagWrite,
        );
        const memdagBaseSnapshot = await baseSnapshotFromCommit(
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

        const newMemdagMutations = await localMutationsGreaterThan(
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
        if (newMemdagMutations.length > 0) {
          const zeroData = await zero?.getTxData?.(newMemdagHeadHash, {
            openLazyRead: memdagWrite,
          });
          for (let i = newMemdagMutations.length - 1; i >= 0; i--) {
            newMemdagHeadHash = (
              await rebaseMutationAndPutCommit(
                newMemdagMutations[i],
                memdagWrite,
                newMemdagHeadHash,
                mutators,
                lc,
                newMemdagMutations[i].meta.clientID,
                formatVersion,
                zeroData,
              )
            ).chunk.hash;
          }
        }

        const newMemdagHeadCommit = await commitFromHash(
          newMemdagHeadHash,
          memdagWrite,
        );
        const diffs = await diffCommits(
          memdagHeadCommit,
          newMemdagHeadCommit,
          memdagWrite,
          diffConfig,
          formatVersion,
        );

        await memdagWrite.setHead(DEFAULT_HEAD_NAME, newMemdagHeadHash);
        return {
          type: 'complete',
          diffs,
          oldHead: memdagHeadCommit.chunk.hash,
          newHead: newMemdagHeadHash,
          newPerdagClientHeadHash: perdagClientGroupHeadHash,
        } as const;
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
    });

  if (result.type === 'aborted') {
    if (result.refreshHashesForRevert) {
      await setRefreshHashes(result.refreshHashesForRevert);
    }
    return undefined;
  }

  // Advance zero here before setting refresh hashes
  // since we must advance before delegating control of the microtask
  // loop.
  zero?.advance(result.oldHead, result.newHead, result.diffs.get('') ?? []);
  await setRefreshHashes([result.newPerdagClientHeadHash]);
  return {
    oldHead: result.oldHead,
    newHead: result.newHead,
    diffs: result.diffs,
  };
}

function shouldAbortRefresh(
  memdagBaseSnapshot: Commit<SnapshotMetaDD31>,
  perdagClientGroupBaseSnapshot: Commit<SnapshotMetaDD31>,
  perdagClientGroupHeadHash: Hash,
): boolean {
  const baseSnapshotCookieCompareResult = compareCookiesForSnapshots(
    memdagBaseSnapshot,
    perdagClientGroupBaseSnapshot,
  );
  return (
    baseSnapshotCookieCompareResult > 0 ||
    (baseSnapshotCookieCompareResult === 0 &&
      perdagClientGroupHeadHash === perdagClientGroupBaseSnapshot.chunk.hash)
  );
}
