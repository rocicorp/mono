import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import type {Chunk} from '../dag/chunk.ts';
import type {LazyStore} from '../dag/lazy-store.ts';
import type {Read, Store, Write} from '../dag/store.ts';
import type {Commit} from '../db/commit.ts';
import {
  DEFAULT_HEAD_NAME,
  type LocalMetaDD31,
  type Meta,
  assertSnapshotCommitDD31,
  baseSnapshotFromCommit,
  commitFromHash,
  commitFromHead,
  compareCookiesForSnapshots,
  localMutationsDD31,
  localMutationsGreaterThan,
} from '../db/commit.ts';
import {rebaseMutationAndPutCommit} from '../db/rebase.ts';
import type * as FormatVersion from '../format-version-enum.ts';
import type {Hash} from '../hash.ts';
import type {ZeroOption, ZeroTxData} from '../replicache-options.ts';
import type {ClientGroupID, ClientID} from '../sync/ids.ts';
import type {MutatorDefs} from '../types.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import {
  type ClientGroup,
  getClientGroup,
  setClientGroup,
} from './client-groups.ts';
import {
  assertClientV6,
  assertHasClientState,
  getClientGroupIDForClient,
  mustGetClient,
  setClient,
} from './clients.ts';
import {GatherMemoryOnlyVisitor} from './gather-mem-only-visitor.ts';

type FormatVersion = Enum<typeof FormatVersion>;

/**
 * Persists the client's memdag state to the client's perdag client group.
 *
 * Persists the base snapshot from memdag to the client's perdag client group,
 * but only if it’s newer than the client's perdag client group’s base snapshot.
 * The base snapshot is persisted by gathering all memory-only chunks in the dag
 * subgraph rooted at the base snapshot's commit and writing them to the perdag.
 * Once the base snapshot is persisted, rebases onto this new base snapshot all
 * local commits from the client's perdag client group that are not already
 * reflected in the base snapshot.
 *
 * Whether or not the base snapshot is persisted, rebases onto the client's
 * perdag client group all memdag local commits not already in the client's
 * perdag client group's history.
 *
 * Also updates the `lastMutationIDs` and `lastServerAckdMutationIDs` properties
 * of the client's client group's entry in the `ClientGroupMap`.
 */
export async function persistDD31(
  lc: LogContext,
  clientID: ClientID,
  memdag: LazyStore,
  perdag: Store,
  mutators: MutatorDefs,
  closed: () => boolean,
  formatVersion: FormatVersion,
  getZeroData: ZeroOption['getTxData'] | undefined,
  onGatherMemOnlyChunksForTest = () => Promise.resolve(),
): Promise<void> {
  if (closed()) {
    return;
  }

  const [perdagLMID, perdagBaseSnapshot, mainClientGroupID] = await withRead(
    perdag,
    async perdagRead => {
      await assertHasClientState(clientID, perdagRead);
      const mainClientGroupID = await getClientGroupIDForClient(
        clientID,
        perdagRead,
      );
      assert(
        mainClientGroupID,
        `No main client group for clientID: ${clientID}`,
      );
      const [, perdagMainClientGroupHeadCommit] = await getClientGroupInfo(
        perdagRead,
        mainClientGroupID,
      );
      const perdagLMID = await perdagMainClientGroupHeadCommit.getMutationID(
        clientID,
        perdagRead,
      );
      const perdagBaseSnapshot = await baseSnapshotFromCommit(
        perdagMainClientGroupHeadCommit,
        perdagRead,
      );
      assertSnapshotCommitDD31(perdagBaseSnapshot);
      return [perdagLMID, perdagBaseSnapshot, mainClientGroupID];
    },
  );

  if (closed()) {
    return;
  }
  const [newMemdagMutations, memdagBaseSnapshot, gatheredChunks] =
    await withRead(memdag, async memdagRead => {
      const memdagHeadCommit = await commitFromHead(
        DEFAULT_HEAD_NAME,
        memdagRead,
      );
      const newMutations = await localMutationsGreaterThan(
        memdagHeadCommit,
        {[clientID]: perdagLMID || 0},
        memdagRead,
      );
      const memdagBaseSnapshot = await baseSnapshotFromCommit(
        memdagHeadCommit,
        memdagRead,
      );
      assertSnapshotCommitDD31(memdagBaseSnapshot);

      let gatheredChunks: ReadonlyMap<Hash, Chunk> | undefined;
      if (
        compareCookiesForSnapshots(memdagBaseSnapshot, perdagBaseSnapshot) > 0
      ) {
        await onGatherMemOnlyChunksForTest();
        // Might need to persist snapshot, we will have to double check
        // after gathering the snapshot chunks from memdag
        const memdagBaseSnapshotHash = memdagBaseSnapshot.chunk.hash;
        // Gather all memory only chunks from base snapshot on the memdag.
        const visitor = new GatherMemoryOnlyVisitor(memdagRead);
        await visitor.visit(memdagBaseSnapshotHash);
        gatheredChunks = visitor.gatheredChunks;
      }

      return [newMutations, memdagBaseSnapshot, gatheredChunks];
    });

  if (closed()) {
    return;
  }

  let memdagBaseSnapshotPersisted = false;
  const zeroDataForMemdagBaseSnapshot =
    getZeroData && (await getZeroData(memdagBaseSnapshot.chunk.hash));

  await withWrite(perdag, async perdagWrite => {
    const [mainClientGroup, latestPerdagMainClientGroupHeadCommit] =
      await getClientGroupInfo(perdagWrite, mainClientGroupID);

    // These initial values for newMainClientGroupHeadHash, mutationIDs,
    // lastServerAckdMutationIDs are correct for the case where the memdag
    // snapshot is *not* persisted.  If the memdag snapshot is persisted
    // these values are overwritten appropriately.
    let newMainClientGroupHeadHash: Hash =
      latestPerdagMainClientGroupHeadCommit.chunk.hash;
    let mutationIDs: Record<ClientID, number> = {
      ...mainClientGroup.mutationIDs,
    };
    let {lastServerAckdMutationIDs} = mainClientGroup;

    if (gatheredChunks) {
      // check if memdag snapshot still newer than perdag snapshot

      const client = await mustGetClient(clientID, perdagWrite);
      assertClientV6(client);

      const latestPerdagBaseSnapshot = await baseSnapshotFromCommit(
        latestPerdagMainClientGroupHeadCommit,
        perdagWrite,
      );
      assertSnapshotCommitDD31(latestPerdagBaseSnapshot);

      // check if memdag snapshot still newer than perdag snapshot
      if (
        compareCookiesForSnapshots(
          memdagBaseSnapshot,
          latestPerdagBaseSnapshot,
        ) > 0
      ) {
        // still newer, persist memdag snapshot by writing chunks
        memdagBaseSnapshotPersisted = true;
        await Promise.all(
          Array.from(gatheredChunks.values(), c => perdagWrite.putChunk(c)),
        );

        await setClient(
          clientID,
          {
            ...client,
            persistHash: memdagBaseSnapshot.chunk.hash,
          },
          perdagWrite,
        );
        // Rebase local mutations from perdag main client group onto new
        // snapshot
        newMainClientGroupHeadHash = memdagBaseSnapshot.chunk.hash;
        const mainClientGroupLocalMutations = await localMutationsDD31(
          mainClientGroup.headHash,
          perdagWrite,
        );

        lastServerAckdMutationIDs = memdagBaseSnapshot.meta.lastMutationIDs;
        mutationIDs = {...lastServerAckdMutationIDs};

        newMainClientGroupHeadHash = await rebase(
          mainClientGroupLocalMutations,
          newMainClientGroupHeadHash,
          perdagWrite,
          mutators,
          mutationIDs,
          lc,
          formatVersion,
          zeroDataForMemdagBaseSnapshot,
        );
      }
    }

    let zeroDataForPerdagHeadCommit: ZeroTxData | undefined;
    if (!memdagBaseSnapshotPersisted) {
      zeroDataForPerdagHeadCommit =
        getZeroData &&
        (await getZeroData(newMainClientGroupHeadHash, {
          openLazySourceRead: perdagWrite,
        }));
    }

    // rebase new memdag mutations onto perdag
    newMainClientGroupHeadHash = await rebase(
      newMemdagMutations,
      newMainClientGroupHeadHash,
      perdagWrite,
      mutators,
      mutationIDs,
      lc,
      formatVersion,
      zeroDataForPerdagHeadCommit ?? zeroDataForMemdagBaseSnapshot,
    );

    const newMainClientGroup = {
      ...mainClientGroup,
      headHash: newMainClientGroupHeadHash,
      mutationIDs,
      lastServerAckdMutationIDs,
    };

    await setClientGroup(mainClientGroupID, newMainClientGroup, perdagWrite);
  });

  if (gatheredChunks && memdagBaseSnapshotPersisted) {
    await withWrite(memdag, memdagWrite =>
      memdagWrite.chunksPersisted([...gatheredChunks.keys()]),
    );
  }
}

async function getClientGroupInfo(
  perdagRead: Read,
  clientGroupID: ClientGroupID,
): Promise<[ClientGroup, Commit<Meta>]> {
  const clientGroup = await getClientGroup(clientGroupID, perdagRead);
  assert(clientGroup, `No client group for clientGroupID: ${clientGroupID}`);
  return [clientGroup, await commitFromHash(clientGroup.headHash, perdagRead)];
}

async function rebase(
  mutations: Commit<LocalMetaDD31>[],
  basis: Hash,
  write: Write,
  mutators: MutatorDefs,
  mutationIDs: Record<ClientID, number>,
  lc: LogContext,
  formatVersion: FormatVersion,
  zeroData: ZeroTxData | undefined,
): Promise<Hash> {
  for (let i = mutations.length - 1; i >= 0; i--) {
    const mutationCommit = mutations[i];
    const {meta} = mutationCommit;
    const newMainHead = await commitFromHash(basis, write);
    if (
      (await mutationCommit.getMutationID(meta.clientID, write)) >
      (await newMainHead.getMutationID(meta.clientID, write))
    ) {
      mutationIDs[meta.clientID] = meta.mutationID;
      basis = (
        await rebaseMutationAndPutCommit(
          mutationCommit,
          write,
          basis,
          mutators,
          lc,
          meta.clientID,
          formatVersion,
          zeroData,
        )
      ).chunk.hash;
    }
  }
  return basis;
}
