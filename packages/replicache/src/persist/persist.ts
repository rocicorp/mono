import type {LogContext} from '@rocicorp/logger';
import {assert} from 'shared/src/asserts.js';
import type * as dag from '../dag/mod.js';
import {assertSnapshotCommitDD31} from '../db/commit.js';
import * as db from '../db/mod.js';
import type {FormatVersion} from '../format-version.js';
import type {Hash} from '../hash.js';
import type {MutatorDefs} from '../replicache.js';
import type {ClientGroupID, ClientID} from '../sync/ids.js';
import {withRead, withWrite} from '../with-transactions.js';
import {ClientGroup, getClientGroup, setClientGroup} from './client-groups.js';
import {
  assertClientV6,
  assertHasClientState,
  getClientGroupIDForClient,
  mustGetClient,
  setClient,
} from './clients.js';
import {GatherMemoryOnlyVisitor} from './gather-mem-only-visitor.js';

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
  memdag: dag.LazyStore,
  perdag: dag.Store,
  mutators: MutatorDefs,
  closed: () => boolean,
  formatVersion: FormatVersion,
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
      const perdagBaseSnapshot = await db.baseSnapshotFromCommit(
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
      const memdagHeadCommit = await db.commitFromHead(
        db.DEFAULT_HEAD_NAME,
        memdagRead,
      );
      const newMutations = await db.localMutationsGreaterThan(
        memdagHeadCommit,
        {[clientID]: perdagLMID || 0},
        memdagRead,
      );
      const memdagBaseSnapshot = await db.baseSnapshotFromCommit(
        memdagHeadCommit,
        memdagRead,
      );
      assertSnapshotCommitDD31(memdagBaseSnapshot);

      let gatheredChunks: ReadonlyMap<Hash, dag.Chunk> | undefined;
      if (
        db.compareCookiesForSnapshots(memdagBaseSnapshot, perdagBaseSnapshot) >
        0
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

      const latestPerdagBaseSnapshot = await db.baseSnapshotFromCommit(
        latestPerdagMainClientGroupHeadCommit,
        perdagWrite,
      );
      assertSnapshotCommitDD31(latestPerdagBaseSnapshot);

      // check if memdag snapshot still newer than perdag snapshot
      if (
        db.compareCookiesForSnapshots(
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
        const mainClientGroupLocalMutations = await db.localMutationsDD31(
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
        );
      }
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
    );

    const newMainClientGroup = {
      ...mainClientGroup,
      headHash: newMainClientGroupHeadHash,
      mutationIDs,
      lastServerAckdMutationIDs,
    };

    await setClientGroup(mainClientGroupID, newMainClientGroup, perdagWrite);
    await perdagWrite.commit();
  });

  if (gatheredChunks && memdagBaseSnapshotPersisted) {
    await withWrite(memdag, async memdagWrite => {
      memdagWrite.chunksPersisted([...gatheredChunks.keys()]);
      await memdagWrite.commit();
    });
  }
}

async function getClientGroupInfo(
  perdagRead: dag.Read,
  clientGroupID: ClientGroupID,
): Promise<[ClientGroup, db.Commit<db.Meta>]> {
  const clientGroup = await getClientGroup(clientGroupID, perdagRead);
  assert(clientGroup, `No client group for clientGroupID: ${clientGroupID}`);
  return [
    clientGroup,
    await db.commitFromHash(clientGroup.headHash, perdagRead),
  ];
}

async function rebase(
  mutations: db.Commit<db.LocalMetaDD31>[],
  basis: Hash,
  write: dag.Write,
  mutators: MutatorDefs,
  mutationIDs: Record<ClientID, number>,
  lc: LogContext,
  formatVersion: FormatVersion,
): Promise<Hash> {
  for (let i = mutations.length - 1; i >= 0; i--) {
    const mutationCommit = mutations[i];
    const {meta} = mutationCommit;
    const newMainHead = await db.commitFromHash(basis, write);
    if (
      (await mutationCommit.getMutationID(meta.clientID, write)) >
      (await newMainHead.getMutationID(meta.clientID, write))
    ) {
      mutationIDs[meta.clientID] = meta.mutationID;
      basis = (
        await db.rebaseMutationAndPutCommit(
          mutationCommit,
          write,
          basis,
          mutators,
          lc,
          meta.clientID,
          formatVersion,
        )
      ).chunk.hash;
    }
  }
  return basis;
}
