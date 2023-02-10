import {assert} from '../asserts';
import type * as dag from '../dag/mod';
import * as db from '../db/mod';
import type * as sync from '../sync/mod';
import {assertHasClientState, getMainClientGroupID} from './clients';
import {GatherVisitor} from './gather-visitor';
import type {MutatorDefs} from '../replicache';
import type {Hash} from '../hash';
import type {LogContext} from '@rocicorp/logger';
import {assertSnapshotCommitDD31} from '../db/commit';
import {ClientGroup, getClientGroup, setClientGroup} from './client-groups';

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
  clientID: sync.ClientID,
  memdag: dag.LazyStore,
  perdag: dag.Store,
  mutators: MutatorDefs,
  closed: () => boolean,
  onGatherMemOnlyChunksForTest = () => Promise.resolve(),
): Promise<void> {
  if (closed()) {
    return;
  }

  const [perdagLMID, perdagBaseSnapshot, mainClientGroupID] =
    await perdag.withRead(async perdagRead => {
      await assertHasClientState(clientID, perdagRead);
      const mainClientGroupID = await getMainClientGroupID(
        clientID,
        perdagRead,
      );
      assert(
        mainClientGroupID,
        `No main client group for clientID: ${clientID}`,
      );
      const [, perdagMainClientGroupHeadCommit] = await getMainClientGroupInfo(
        perdagRead,
        mainClientGroupID,
      );
      const perdagLMID = await perdagMainClientGroupHeadCommit.getMutationID(
        clientID,
        perdagRead,
      );
      return [
        perdagLMID,
        await db.baseSnapshotFromCommit(
          perdagMainClientGroupHeadCommit,
          perdagRead,
        ),
        mainClientGroupID,
      ];
    });

  if (closed()) {
    return;
  }
  const [newMemdagMutations, memdagBaseSnapshot] = await memdag.withRead(
    async memdagRead => {
      const memdagHeadCommit = await db.commitFromHead(
        db.DEFAULT_HEAD_NAME,
        memdagRead,
      );
      const newMutations = await db.localMutationsGreaterThan(
        memdagHeadCommit,
        {[clientID]: perdagLMID || 0},
        memdagRead,
      );
      return [
        newMutations,
        await db.baseSnapshotFromCommit(memdagHeadCommit, memdagRead),
      ];
    },
  );

  if (
    db.compareCookiesForSnapshots(memdagBaseSnapshot, perdagBaseSnapshot) > 0
  ) {
    await onGatherMemOnlyChunksForTest();
    // Might need to persist snapshot, we will have to double check
    // after gathering the snapshot chunks from memdag
    const memdagBaseSnapshotHash = memdagBaseSnapshot.chunk.hash;
    // Gather all memory only chunks from base snapshot on the memdag.
    const gatheredChunks = await gatherMemOnlyChunks(
      memdag,
      memdagBaseSnapshotHash,
    );
    let memdagBaseSnapshotPersisted = false;
    if (closed()) {
      return;
    }
    await perdag.withWrite(async perdagWrite => {
      // check if memdag snapshot still newer than perdag snapshot
      const [mainClientGroup, latestPerdagMainClientGroupHeadCommit] =
        await getMainClientGroupInfo(perdagWrite, mainClientGroupID);
      let mutationIDs;
      let {lastServerAckdMutationIDs} = mainClientGroup;
      const latestPerdagBaseSnapshot = await db.baseSnapshotFromCommit(
        latestPerdagMainClientGroupHeadCommit,
        perdagWrite,
      );
      let newMainClientGroupHeadHash: Hash;
      // check if memdag snapshot still newer than perdag snapshot
      if (
        db.compareCookiesForSnapshots(
          memdagBaseSnapshot,
          latestPerdagBaseSnapshot,
        ) > 0
      ) {
        // still newer, persist memdag snapshot by writing chunks
        await Promise.all(
          Array.from(gatheredChunks.values(), c => perdagWrite.putChunk(c)),
        );
        memdagBaseSnapshotPersisted = true;
        // Rebase local mutations from perdag main client group onto new
        // snapshot
        newMainClientGroupHeadHash = memdagBaseSnapshotHash;
        const mainClientGroupLocalMutations = await db.localMutationsDD31(
          mainClientGroup.headHash,
          perdagWrite,
        );
        assertSnapshotCommitDD31(memdagBaseSnapshot);
        lastServerAckdMutationIDs = memdagBaseSnapshot.meta.lastMutationIDs;
        mutationIDs = {...lastServerAckdMutationIDs};

        newMainClientGroupHeadHash = await rebase(
          mainClientGroupLocalMutations,
          newMainClientGroupHeadHash,
          perdagWrite,
          mutators,
          mutationIDs,
          lc,
        );
      } else {
        newMainClientGroupHeadHash =
          latestPerdagMainClientGroupHeadCommit.chunk.hash;
        mutationIDs = {...mainClientGroup.mutationIDs};
      }
      // persist new memdag mutations
      newMainClientGroupHeadHash = await rebase(
        newMemdagMutations,
        newMainClientGroupHeadHash,
        perdagWrite,
        mutators,
        mutationIDs,
        lc,
      );
      await setClientGroup(
        mainClientGroupID,
        {
          ...mainClientGroup,
          headHash: newMainClientGroupHeadHash,
          mutationIDs,
          lastServerAckdMutationIDs,
        },
        perdagWrite,
      );
      await perdagWrite.commit();
    });
    if (memdagBaseSnapshotPersisted) {
      await memdag.chunksPersisted(gatheredChunks.keys());
    }
  } else {
    if (closed()) {
      return;
    }

    lc.debug?.(
      'memdag base snapshot is older than (or same as) perdag base snapshot',
      memdagBaseSnapshot.meta.cookieJSON,
      perdagBaseSnapshot.meta.cookieJSON,
    );

    // no need to persist snapshot, persist new memdag mutations
    await perdag.withWrite(async perdagWrite => {
      const [mainClientGroup, latestPerdagMainClientGroupHeadCommit] =
        await getMainClientGroupInfo(perdagWrite, mainClientGroupID);
      const mutationIDs = {...mainClientGroup.mutationIDs};
      const newMainClientGroupHeadHash = await rebase(
        newMemdagMutations,
        latestPerdagMainClientGroupHeadCommit.chunk.hash,
        perdagWrite,
        mutators,
        mutationIDs,
        lc,
      );
      await setClientGroup(
        mainClientGroupID,
        {
          ...mainClientGroup,
          headHash: newMainClientGroupHeadHash,
          mutationIDs,
        },
        perdagWrite,
      );
      await perdagWrite.commit();
    });
  }
}

async function getMainClientGroupInfo(
  perdagRead: dag.Read,
  clientGroupID: sync.ClientGroupID,
): Promise<[ClientGroup, db.Commit<db.Meta>]> {
  const mainClientGroup = await getClientGroup(clientGroupID, perdagRead);
  assert(
    mainClientGroup,
    `No main client group for clientGroupID: ${clientGroupID}`,
  );
  return [
    mainClientGroup,
    await db.commitFromHash(mainClientGroup.headHash, perdagRead),
  ];
}

async function gatherMemOnlyChunks(
  memdag: dag.LazyStore,
  baseSnapshotHash: Hash,
): Promise<ReadonlyMap<Hash, dag.Chunk>> {
  return await memdag.withRead(async dagRead => {
    const visitor = new GatherVisitor(dagRead);
    await visitor.visitCommit(baseSnapshotHash);
    return visitor.gatheredChunks;
  });
}

async function rebase(
  mutations: db.Commit<db.LocalMetaDD31>[],
  basis: Hash,
  write: dag.Write,
  mutators: MutatorDefs,
  mutationIDs: Record<sync.ClientID, number>,
  lc: LogContext,
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
        )
      ).chunk.hash;
    }
  }
  return basis;
}
