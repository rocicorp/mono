import {assert} from '../asserts.js';
import type * as dag from '../dag/mod.js';
import type * as sync from '../sync/mod.js';
import * as db from '../db/mod.js';
import type {Hash} from '../hash.js';
import {assertHasClientState, setClient} from './clients.js';
import {GatherMemoryOnlyVisitor} from './gather-mem-only-visitor.js';
import {assertSnapshotMetaSDD} from '../db/commit.js';
import {persistDD31} from './persist-dd31.js';
import type {LogContext} from '@rocicorp/logger';
import type {MutatorDefs} from '../replicache.js';

/**
 * Persists the client's 'main' head memdag state to the perdag.
 *
 * @param clientID
 * @param memdag Dag to gather memory-only chunks from.
 * @param perdag Dag to write gathered memory-only chunks to.
 * @returns A promise that is fulfilled when persist completes successfully,
 * or is rejected if the persist fails.
 */
export function persist(
  lc: LogContext,
  clientID: sync.ClientID,
  memdag: dag.LazyStore,
  perdag: dag.Store,
  mutators: MutatorDefs,
  closed: () => boolean,
): Promise<void> {
  if (DD31) {
    return persistDD31(lc, clientID, memdag, perdag, mutators, closed);
  }

  return persistSDD(clientID, memdag, perdag, closed);
}

export async function persistSDD(
  clientID: sync.ClientID,
  memdag: dag.LazyStore,
  perdag: dag.Store,
  closed: () => boolean,
): Promise<void> {
  if (closed()) {
    return;
  }

  // Start checking if client exists while we do other async work
  const clientExistsCheckP = perdag.withRead(read =>
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

async function gatherMemOnlyChunks(
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
  return await memdag.withRead(async dagRead => {
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
  await perdag.withWrite(async dagWrite => {
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
