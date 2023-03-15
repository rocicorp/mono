import {assert} from 'shared';
import type * as dag from '../dag/mod.js';
import type * as sync from '../sync/mod.js';
import * as db from '../db/mod.js';
import type {Hash} from '../hash.js';
import {assertHasClientState, setClient} from './clients.js';
import {GatherMemoryOnlyVisitor} from './gather-mem-only-visitor.js';
import {assertSnapshotMetaSDD} from '../db/commit.js';
import {withRead, withWrite} from '../with-transactions.js';

/**
 * Persists the client's 'main' head memdag state to the perdag.
 *
 * @param clientID
 * @param memdag Dag to gather memory-only chunks from.
 * @param perdag Dag to write gathered memory-only chunks to.
 * @param closed A function that returns true if the store has been closed.
 * @returns A promise that is fulfilled when persist completes successfully,
 * or is rejected if the persist fails.
 */
export async function persistSDD(
  clientID: sync.ClientID,
  memdag: dag.LazyStore,
  perdag: dag.Store,
  closed: () => boolean,
): Promise<void> {
  // This is only used for testing.
  if (closed()) {
    return;
  }

  // Start checking if client exists while we do other async work
  const clientExistsCheckP = withRead(perdag, read =>
    assertHasClientState(clientID, read),
  );

  if (closed()) {
    return;
  }

  const [gatheredChunks, mainHeadHash, mutationID, lastMutationID] =
    await gatherMemOnlyChunks(memdag, clientID);

  await clientExistsCheckP;

  if (gatheredChunks.size === 0) {
    // Nothing to persist
    return;
  }

  if (closed()) {
    return;
  }

  await writeChunks(
    perdag,
    gatheredChunks,
    mainHeadHash,
    clientID,
    mutationID,
    lastMutationID,
  );
  await memdag.chunksPersisted(gatheredChunks.keys());
}

function gatherMemOnlyChunks(
  memdag: dag.LazyStore,
  clientID: sync.ClientID,
): Promise<
  [
    map: ReadonlyMap<Hash, dag.Chunk>,
    hash: Hash,
    mutationID: number,
    lastMutationID: number,
  ]
> {
  return withRead(memdag, async dagRead => {
    const mainHeadHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    assert(mainHeadHash);
    const visitor = new GatherMemoryOnlyVisitor(dagRead);
    await visitor.visitCommit(mainHeadHash);
    const headCommit = await db.commitFromHash(mainHeadHash, dagRead);
    const baseSnapshotCommit = await db.baseSnapshotFromHash(
      mainHeadHash,
      dagRead,
    );
    const {meta} = baseSnapshotCommit;
    assertSnapshotMetaSDD(meta);
    return [
      visitor.gatheredChunks,
      mainHeadHash,
      await headCommit.getMutationID(clientID, dagRead),
      meta.lastMutationID,
    ];
  });
}

async function writeChunks(
  perdag: dag.Store,
  chunks: ReadonlyMap<Hash, dag.Chunk>,
  mainHeadHash: Hash,
  clientID: sync.ClientID,
  mutationID: number,
  lastMutationID: number,
): Promise<void> {
  await withWrite(perdag, async dagWrite => {
    const ps: Promise<unknown>[] = [];

    ps.push(
      setClient(
        clientID,
        {
          heartbeatTimestampMs: Date.now(),
          headHash: mainHeadHash,
          mutationID,
          lastServerAckdMutationID: lastMutationID,
        },
        dagWrite,
      ),
    );

    for (const chunk of chunks.values()) {
      ps.push(dagWrite.putChunk(chunk));
    }

    await Promise.all(ps);

    await dagWrite.commit();
  });
}
