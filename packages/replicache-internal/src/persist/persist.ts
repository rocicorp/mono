import {assert} from '../asserts';
import type * as dag from '../dag/mod';
import * as db from '../db/mod';
import * as sync from '../sync/mod';
import {Hash, hashOf} from '../hash';
import type {ClientID} from '../sync/client-id';
import {assertHasClientState, updateClients} from './clients';
import {ComputeHashTransformer, FixedChunks} from './compute-hash-transformer';
import {GatherVisitor} from './gather-visitor';
import {FixupTransformer} from './fixup-transformer';

/**
 * Computes permanent hashes from all temp chunks in `memdag` and writes them
 * to `perdag`.  Replaces in `memdag` all temp chunks written with chunks with
 * permanent hashes.
 *
 * @param clientID
 * @param memdag Dag to gather temp chunks from.
 * @param perdag Dag to write gathered temp chunks to.
 * @returns A promise that is fulfilled when persist completes successfully,
 * or is rejected if the persist fails.
 */
export async function persist(
  clientID: ClientID,
  memdag: dag.Store,
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

  // 1. Gather all temp chunks from main head on the memdag.
  const [gatheredChunks, mainHeadTempHash, mutationID, lastMutationID] =
    await gatherTempChunks(memdag);

  if (gatheredChunks.size === 0) {
    // Nothing to persist
    await clientExistsCheckP;
    return;
  }

  // 2. Compute the hashes for these gathered chunks.
  const computeHashesP = computeHashes(gatheredChunks, mainHeadTempHash);

  await clientExistsCheckP;

  const [fixedChunks, mappings, mainHeadHash] = await computeHashesP;

  if (closed()) {
    return;
  }

  // 3. write chunks to perdag.
  await writeFixedChunks(
    perdag,
    fixedChunks,
    mainHeadHash,
    clientID,
    mutationID,
    lastMutationID,
  );

  // 4. fixup the memdag with the new hashes.
  await fixupMemdagWithNewHashes(memdag, mappings);
}

async function gatherTempChunks(
  memdag: dag.Store,
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
    const baseSnapshotCommit = await db.baseSnapshot(mainHeadHash, dagRead);
    return [
      visitor.gatheredChunks,
      mainHeadHash,
      headCommit.mutationID,
      baseSnapshotCommit.meta.lastMutationID,
    ];
  });
}

async function computeHashes(
  gatheredChunks: ReadonlyMap<Hash, dag.Chunk>,
  mainHeadTempHash: Hash,
): Promise<[FixedChunks, ReadonlyMap<Hash, Hash>, Hash]> {
  const transformer = new ComputeHashTransformer(gatheredChunks, hashOf);
  const mainHeadHash = await transformer.transformCommit(mainHeadTempHash);
  const {fixedChunks, mappings} = transformer;
  return [fixedChunks, mappings, mainHeadHash];
}

async function fixupMemdagWithNewHashes(
  memdag: dag.Store,
  mappings: ReadonlyMap<Hash, Hash>,
) {
  await memdag.withWrite(async dagWrite => {
    for (const headName of [db.DEFAULT_HEAD_NAME, sync.SYNC_HEAD_NAME]) {
      const headHash = await dagWrite.getHead(headName);
      if (!headHash) {
        if (headName === sync.SYNC_HEAD_NAME) {
          // It is OK to not have a sync head.
          break;
        }
        throw new Error(`No head found for ${headName}`);
      }
      const transformer = new FixupTransformer(dagWrite, mappings);
      const newHeadHash = await transformer.transformCommit(headHash);
      await dagWrite.setHead(headName, newHeadHash);
    }
    await dagWrite.commit();
  });
}

async function writeFixedChunks(
  perdag: dag.Store,
  fixedChunks: FixedChunks,
  mainHeadHash: Hash,
  clientID: string,
  mutationID: number,
  lastMutationID: number,
) {
  const chunksToPut = fixedChunks.values();
  await updateClients(clients => {
    return {
      clients: new Map(clients).set(clientID, {
        heartbeatTimestampMs: Date.now(),
        headHash: mainHeadHash,
        mutationID,
        lastServerAckdMutationID: lastMutationID,
      }),
      chunksToPut,
    };
  }, perdag);
}
