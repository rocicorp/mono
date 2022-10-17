import {assert} from '../asserts';
import type * as dag from '../dag/mod';
import * as db from '../db/mod';
import type * as sync from '../sync/mod';
import {assertHasClientState, getMainBranchID} from './clients';
import {GatherVisitor} from './gather-visitor';
import type {MutatorDefs} from '../replicache';
import type {Hash} from '../hash';
import type {LogContext} from '@rocicorp/logger';
import {assertLocalMetaDD31, assertSnapshotCommitDD31} from '../db/commit';
import {Branch, getBranch, setBranch} from './branches';

/**
 * Persists the client's memdag state to the client's perdag branch.
 *
 * Persists the base snapshot from memdag to the client's perdag branch,
 * but only if it’s newer than the client's perdag branch’s base snapshot. The
 * base snapshot is persisted by gathering all memory-only chunks in the dag
 * subgraph rooted at the base snapshot's commit and writing them to
 * the perdag.  Once the base snapshot is persisted, rebases onto this new base
 * snapshot all local commits from the client's perdag branch that are not
 * already reflected in the base snapshot.
 *
 * Whether or not the base snapshot is persisted, rebases onto the client's
 * perdag branch all memdag local commits not already in the client's perdag
 * branch's history.
 *
 * Also updates the `lastMutationIDs` and `lastServerAckdMutationIDs` properties
 * of the client's branch's entry in the `BranchMap`.
 */
export async function persistDD31(
  lc: LogContext,
  clientID: sync.ClientID,
  chunkLocationTracker: dag.ChunkLocationTracker,
  memdag: dag.Store,
  perdag: dag.Store,
  mutators: MutatorDefs,
  closed: () => boolean,
  onGatherMemOnlyChunksForTest = async () => {
    return;
  },
): Promise<void> {
  if (closed()) {
    return;
  }

  const [perdagLMID, perdagBaseSnapshot, mainBranchID] = await perdag.withRead(
    async perdagRead => {
      await assertHasClientState(clientID, perdagRead);
      const mainBranchID = await getMainBranchID(clientID, perdagRead);
      assert(mainBranchID, `No main branch id for clientID: ${clientID}`);
      const [, perdagMainBranchHeadCommit] = await getMainBranchInfo(
        perdagRead,
        mainBranchID,
      );
      const perdagLMID = await perdagMainBranchHeadCommit.getMutationID(
        clientID,
        perdagRead,
      );
      return [
        perdagLMID,
        await db.baseSnapshotFromCommit(perdagMainBranchHeadCommit, perdagRead),
        mainBranchID,
      ];
    },
  );

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
    // Might need to perist snapshot, we will have to double check
    // after gathering the snapshot chunks from memdag
    const memdagBaseSnapshotHash = memdagBaseSnapshot.chunk.hash;
    // Gather all memory only chunks from base snapshot on the memdag.
    const gatheredChunks = await gatherMemOnlyChunks(
      chunkLocationTracker,
      memdag,
      memdagBaseSnapshotHash,
    );
    let memdagBaseSnapshotPersisted = false;
    await perdag.withWrite(async perdagWrite => {
      // check if memdag snapshot still newer than perdag snapshot
      const [mainBranch, latestPerdagMainBranchHeadCommit] =
        await getMainBranchInfo(perdagWrite, mainBranchID);
      let mutationIDs;
      let {lastServerAckdMutationIDs} = mainBranch;
      const latestPerdagBaseSnapshot = await db.baseSnapshotFromCommit(
        latestPerdagMainBranchHeadCommit,
        perdagWrite,
      );
      let newMainBranchHeadHash: Hash;
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
        // Rebase local mutations from perdag main branch onto new snapshot
        newMainBranchHeadHash = memdagBaseSnapshotHash;
        const mainBranchLocalMutations = await db.localMutations(
          mainBranch.headHash,
          perdagWrite,
        );
        assertSnapshotCommitDD31(memdagBaseSnapshot);
        lastServerAckdMutationIDs = memdagBaseSnapshot.meta.lastMutationIDs;
        mutationIDs = {...lastServerAckdMutationIDs};

        newMainBranchHeadHash = await rebase(
          mainBranchLocalMutations,
          newMainBranchHeadHash,
          perdagWrite,
          mutators,
          mutationIDs,
          lc,
        );
      } else {
        newMainBranchHeadHash = latestPerdagMainBranchHeadCommit.chunk.hash;
        mutationIDs = {...mainBranch.mutationIDs};
      }

      if (memdagBaseSnapshotPersisted) {
        await chunkLocationTracker.chunksPersisted([...gatheredChunks.keys()]);
      }

      // persist new memdag mutations
      newMainBranchHeadHash = await rebase(
        newMemdagMutations,
        newMainBranchHeadHash,
        perdagWrite,
        mutators,
        mutationIDs,
        lc,
      );
      await setBranch(
        mainBranchID,
        {
          ...mainBranch,
          headHash: newMainBranchHeadHash,
          mutationIDs,
          lastServerAckdMutationIDs,
        },
        perdagWrite,
      );
      await perdagWrite.commit();
    });
  } else {
    // no need to persist snapshot, persist new memdag mutations
    await perdag.withWrite(async perdagWrite => {
      const [mainBranch, latestPerdagMainBranchHeadCommit] =
        await getMainBranchInfo(perdagWrite, mainBranchID);
      const mutationIDs = {...mainBranch.mutationIDs};
      const newMainBranchHeadHash = await rebase(
        newMemdagMutations,
        latestPerdagMainBranchHeadCommit.chunk.hash,
        perdagWrite,
        mutators,
        mutationIDs,
        lc,
      );
      await setBranch(
        mainBranchID,
        {
          ...mainBranch,
          headHash: newMainBranchHeadHash,
          mutationIDs,
        },
        perdagWrite,
      );
      await perdagWrite.commit();
    });
  }
}

async function getMainBranchInfo(
  perdagRead: dag.Read,
  branchID: sync.BranchID,
): Promise<[Branch, db.Commit<db.Meta>]> {
  const mainBranch = await getBranch(branchID, perdagRead);
  assert(mainBranch, `No main branch for branchID: ${branchID}`);
  return [mainBranch, await db.commitFromHash(mainBranch.headHash, perdagRead)];
}

async function gatherMemOnlyChunks(
  chunkLocationTracker: dag.ChunkLocationTracker,
  memdag: dag.Store,
  baseSnapshotHash: Hash,
): Promise<ReadonlyMap<Hash, dag.Chunk>> {
  return await memdag.withRead(async dagRead => {
    assert(baseSnapshotHash);
    const visitor = new GatherVisitor(chunkLocationTracker, dagRead);
    await visitor.visitCommit(baseSnapshotHash);
    return visitor.gatheredChunks;
  });
}

async function rebase(
  mutations: db.Commit<db.LocalMetaSDD>[],
  basis: Hash,
  write: dag.Write,
  mutators: MutatorDefs,
  mutationIDs: Record<sync.ClientID, number>,
  lc: LogContext,
): Promise<Hash> {
  for (let i = mutations.length - 1; i >= 0; i--) {
    const mutationCommit = mutations[i];
    const {meta} = mutationCommit;
    assertLocalMetaDD31(meta);
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
