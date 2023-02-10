import {assert} from '../asserts';
import type * as dag from '../dag/mod';
import type * as sync from '../sync/mod';
import * as db from '../db/mod';
import type {Hash} from '../hash';
import {assertHasClientState, setClient} from './clients';
import {GatherVisitor} from './gather-visitor';
import {assertSnapshotMeta} from '../db/commit.js';
import {persistDD31} from './persist-dd31';
import type {LogContext} from '@rocicorp/logger';
import type {MutatorDefs} from '../replicache';

/**
 * Persists the client's 'main' head memdag state to the perdag.
 *
 * @param clientID
 * @param memdag Dag to gather memory-only chunks from.
 * @param perdag Dag to write gathered memory-only chunks to.
 * @returns A promise that is fulfilled when persist completes successfully,
 * or is rejected if the persist fails.
 */
export async function persist(
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

  console.log(gatheredChunks.size);

  await clientExistsCheckP;

  if (gatheredChunks.size === 0) {
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
  await memdag.chunksPersisted([...gatheredChunks.keys()]);
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
    const visitor = new GatherVisitor(dagRead);
    await visitor.visitCommit(mainHeadHash);
    const headCommit = await db.commitFromHash(mainHeadHash, dagRead);
    const baseSnapshotCommit = await db.baseSnapshotFromHash(
      mainHeadHash,
      dagRead,
    );
    const {meta} = baseSnapshotCommit;
    assertSnapshotMeta(meta);
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
