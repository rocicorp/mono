import {assert} from 'shared/src/asserts.js';
import type {Chunk} from '../dag/chunk.js';
import type {LazyStore} from '../dag/lazy-store.js';
import type {Store} from '../dag/store.js';
import {
  DEFAULT_HEAD_NAME,
  assertSnapshotMetaSDD,
  baseSnapshotFromHash,
  commitFromHash,
} from '../db/commit.js';
import type {Hash} from '../hash.js';
import type {ClientID} from '../sync/ids.js';
import {withRead, withWrite} from '../with-transactions.js';
import {assertHasClientState, setClient} from './clients.js';
import {GatherMemoryOnlyVisitor} from './gather-mem-only-visitor.js';

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
  clientID: ClientID,
  memdag: LazyStore,
  perdag: Store,
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
  await withWrite(memdag, w => w.chunksPersisted([...gatheredChunks.keys()]));
}

function gatherMemOnlyChunks(
  memdag: LazyStore,
  clientID: ClientID,
): Promise<
  [
    map: ReadonlyMap<Hash, Chunk>,
    hash: Hash,
    mutationID: number,
    lastMutationID: number,
  ]
> {
  return withRead(memdag, async dagRead => {
    const mainHeadHash = await dagRead.getHead(DEFAULT_HEAD_NAME);
    assert(mainHeadHash);
    const visitor = new GatherMemoryOnlyVisitor(dagRead);
    await visitor.visit(mainHeadHash);
    const headCommit = await commitFromHash(mainHeadHash, dagRead);
    const baseSnapshotCommit = await baseSnapshotFromHash(
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
  perdag: Store,
  chunks: ReadonlyMap<Hash, Chunk>,
  mainHeadHash: Hash,
  clientID: ClientID,
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
  });
}
