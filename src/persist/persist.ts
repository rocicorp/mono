import {assertNotUndefined, assertNumber} from '../asserts';
import type * as dag from '../dag/mod';
import * as db from '../db/mod';
import {Hash, hashOf} from '../hash';
import type {ClientID} from '../sync/client-id';
import {assertHasClientState, getClients, updateClientsDD31} from './clients';
import {ComputeHashTransformer, FixedChunks} from './compute-hash-transformer';
import {GatherVisitor} from './gather-visitor';
import {
  assertLocalMetaDD31,
  assertSnapshotMeta,
  assertSnapshotMetaDD31,
  localMutationsGreaterThan,
} from '../db/commit.js';
import type {ReadonlyJSONValue} from '../json';
import {fromInternalValue, FromInternalValueReason} from '../internal-value';
import {rebaseMutation} from '../sync/rebase';
import type {MutatorDefs} from '../mod';
import type {LogContext} from '@rocicorp/logger';
import type {DiffsMap} from '../sync/mod';
import * as btree from '../btree/mod';
import {addDiffsForIndexes} from '../sync/pull';

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
  mutators: MutatorDefs,
  lc: LogContext,
  compareCookies: (c1: ReadonlyJSONValue, c2: ReadonlyJSONValue) => number,
  closed: () => boolean,
): Promise<void> {
  console.log('persist');
  if (closed()) {
    return;
  }

  // alternative approach... try to first refresh (including rebasing our mutations
  // and handling the case of our memdag having newer basesnapshot)
  // on top and then try to persist the result to the perdag
  // but what if we have newer base snapshot?

  const [perdagLMID, perdagBaseSnapshot] = await perdag.withRead(async read => {
    const mainHeadHash = await read.getHead(db.DEFAULT_HEAD_NAME);
    if (mainHeadHash === undefined) {
      return [undefined, undefined];
    }
    const perdagLMID = await (
      await db.commitFromHead(db.DEFAULT_HEAD_NAME, read)
    ).getMutationID(clientID, read);
    return [perdagLMID, await db.baseSnapshot(mainHeadHash, read)];
  });

  const [newMutations, memdagBaseSnapshot] = await memdag.withRead(
    async read => {
      const mainHeadHash = await read.getHead(db.DEFAULT_HEAD_NAME);
      assertNotUndefined(mainHeadHash);
      const newMutations = await localMutationsGreaterThan(
        mainHeadHash,
        {[clientID]: perdagLMID || 0},
        read,
      );
      return [newMutations, await db.baseSnapshot(mainHeadHash, read)];
    },
  );

  // Start checking if client exists while we do other async work
  const clientExistsCheckP = perdag.withRead(read =>
    assertHasClientState(clientID, read),
  );

  if (
    perdagBaseSnapshot === undefined ||
    compareCookies(
      fromInternalValue(
        memdagBaseSnapshot.meta.cookieJSON,
        FromInternalValueReason.PersistCompareCookies,
      ),
      fromInternalValue(
        perdagBaseSnapshot.meta.cookieJSON,
        FromInternalValueReason.PersistCompareCookies,
      ),
    ) > 0
  ) {
    console.log('might need to persist snapshot');
    // Might need to perist snapshot
    // 1. Gather all temp chunks from base snapshot on the memdag.
    const [gatheredChunks, memdagBaseSnapshotTempHash] = await gatherTempChunks(
      memdag,
      memdagBaseSnapshot,
      clientID,
    );

    if (gatheredChunks.size === 0 && newMutations.length === 0) {
      // Nothing to persist
      await clientExistsCheckP;
      return;
    }
    // 2. Compute the hashes for these gathered chunks.
    const computeHashesP = computeHashes(
      gatheredChunks,
      memdagBaseSnapshotTempHash,
    );

    await clientExistsCheckP;

    const [fixedChunks, , memdagBaseSnapshotHash] = await computeHashesP;

    if (closed()) {
      return;
    }

    await perdag.withWrite(async write => {
      //check if still newer basesnapshot
      const mainHeadHash = await write.getHead(db.DEFAULT_HEAD_NAME);
      let newMainHeadHash;
      if (
        mainHeadHash === undefined ||
        compareCookies(
          fromInternalValue(
            memdagBaseSnapshot.meta.cookieJSON,
            FromInternalValueReason.PersistCompareCookies,
          ),
          fromInternalValue(
            (await db.baseSnapshot(mainHeadHash, write)).meta.cookieJSON,
            FromInternalValueReason.PersistCompareCookies,
          ),
        ) > 0
      ) {
        console.log('persisting snapshot');
        const chunksToPutPromises: Promise<void>[] = [];
        if (gatheredChunks) {
          for (const chunk of fixedChunks.values()) {
            chunksToPutPromises.push(write.putChunk(chunk));
          }
        }
        await Promise.all([...chunksToPutPromises]);
        newMainHeadHash = memdagBaseSnapshotHash;
        if (mainHeadHash !== undefined) {
          const localMutations = await db.localMutations(mainHeadHash, write);
          newMainHeadHash = await rebase(
            localMutations,
            newMainHeadHash,
            write,
            mutators,
            lc,
          );
        }
      } else {
        newMainHeadHash = mainHeadHash;
      }
      console.log('persisting new mutations', newMutations.length);
      newMainHeadHash = await rebase(
        newMutations,
        newMainHeadHash,
        write,
        mutators,
        lc,
      );
      await write.setHead(db.DEFAULT_HEAD_NAME, newMainHeadHash);
      await write.commit();
    });
  } else {
    console.log(
      'no need to persist snapshot, persisting new mutations',
      newMutations.length,
    );
    await perdag.withWrite(async write => {
      const mainHeadHash = await write.getHead(db.DEFAULT_HEAD_NAME);
      assertNotUndefined(mainHeadHash);
      const newMainHeadHash = await rebase(
        newMutations,
        mainHeadHash,
        write,
        mutators,
        lc,
      );
      await write.setHead(db.DEFAULT_HEAD_NAME, newMainHeadHash);
      await write.commit();
    });
  }
}

async function rebase(
  mutations: db.Commit<db.LocalMeta>[],
  basis: Hash,
  write: dag.Write,
  mutators: MutatorDefs,
  lc: LogContext,
): Promise<Hash> {
  for (const commit of mutations.reverse()) {
    const {meta} = commit;
    assertLocalMetaDD31(meta);
    const newMainHead = await db.commitFromHash(basis, write);
    if (
      (await commit.getMutationID(meta.clientID, write)) >
      (await newMainHead.getMutationID(meta.clientID, write))
    ) {
      basis = await rebaseMutation(
        commit,
        write,
        basis,
        mutators,
        lc,
        meta.clientID,
        false,
      );
    }
  }
  return basis;
}

async function gatherTempChunks(
  memdag: dag.Store,
  commit: db.Commit<db.Meta>,
  clientID: ClientID,
): Promise<
  [
    map: ReadonlyMap<Hash, dag.Chunk>,
    hash: Hash,
    mutationID: number,
    lastMutationID: number,
  ]
> {
  return await memdag.withRead(async dagRead => {
    const commitHash = commit.chunk.hash;
    const visitor = new GatherVisitor(dagRead);
    await visitor.visitCommit(commit.chunk.hash);
    const baseSnapshotCommit = await db.baseSnapshot(commitHash, dagRead);
    let lastMutationID: number;
    const {meta} = baseSnapshotCommit;
    if (DD31) {
      assertSnapshotMetaDD31(meta);
      const lmid = meta.lastMutationIDs[clientID];
      assertNumber(lmid);
      lastMutationID = lmid;
    } else {
      assertSnapshotMeta(meta);
      lastMutationID = meta.lastMutationID;
    }
    return [
      visitor.gatheredChunks,
      commitHash,
      await commit.getMutationID(clientID, dagRead),
      lastMutationID,
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

// async function fixupMemdagWithNewHashes(
//   memdag: dag.Store,
//   mappings: ReadonlyMap<Hash, Hash>,
// ) {
//   await memdag.withWrite(async dagWrite => {
//     for (const headName of [db.DEFAULT_HEAD_NAME, sync.SYNC_HEAD_NAME]) {
//       const headHash = await dagWrite.getHead(headName);
//       if (!headHash) {
//         if (headName === sync.SYNC_HEAD_NAME) {
//           // It is OK to not have a sync head.
//           break;
//         }
//         throw new Error(`No head found for ${headName}`);
//       }
//       const transformer = new FixupTransformer(dagWrite, mappings);
//       const newHeadHash = await transformer.transformCommit(headHash);
//       await dagWrite.setHead(headName, newHeadHash);
//     }
//     await dagWrite.commit();
//   });
// }

// async function writeFixedChunks(
//   perdag: dag.Store,
//   fixedChunks: FixedChunks,
//   mainHeadHash: Hash,
//   clientID: string,
//   mutationID: number,
//   lastMutationID: number,
// ) {
//   const chunksToPut = fixedChunks.values();
//   await updateClients(clients => {
//     return {
//       clients: new Map(clients).set(clientID, {
//         heartbeatTimestampMs: Date.now(),
//         headHash: mainHeadHash,
//         mutationID,
//         lastServerAckdMutationID: lastMutationID,
//       }),
//       chunksToPut,
//     };
//   }, perdag);
// }

export async function refresh(
  clientID: ClientID,
  memdag: dag.Store,
  perdag: dag.Store,
  mutators: MutatorDefs,
  lc: LogContext,
  compareCookies: (c1: ReadonlyJSONValue, c2: ReadonlyJSONValue) => number,
): Promise<{newHead: Hash; diffs: DiffsMap} | undefined> {
  console.log('refresh');
  const [perdagMainHead, perdagLMID, perdagBaseSnapshot] =
    await perdag.withWrite(async write => {
      const perdagMainHead = await write.getHead(db.DEFAULT_HEAD_NAME);
      if (perdagMainHead === undefined) {
        return [undefined, 0];
      }
      const perdagLMID = await (
        await db.commitFromHash(perdagMainHead, write)
      ).getMutationID(clientID, write);
      const clients = await getClients(write);
      const newClients = new Map(clients);
      const client = clients.get(clientID);
      assertNotUndefined(client); // should throw client not found
      const updatedClient = {
        ...client,
        tempRefreshHash: perdagMainHead,
      };
      newClients.set(clientID, updatedClient);
      await updateClientsDD31(newClients, write);
      await write.commit();
      return [
        perdagMainHead,
        perdagLMID,
        await db.baseSnapshot(perdagMainHead, write),
      ];
    });
  if (perdagMainHead === undefined) {
    return undefined;
  }

  // Need to pull this head into memdag, but can't have it disappear if
  // perdag moves forward while we're rebasing in memdag. Can't use client
  // headHash until our rebase in memdag is complete, because if rebase fails,
  // then nothing is keeping client's main alive in perdag.
  const result = await memdag.withWrite(async write => {
    const currMemdagHead = await write.getHead(db.DEFAULT_HEAD_NAME);
    assertNotUndefined(currMemdagHead);
    if (
      compareCookies(
        fromInternalValue(
          (await db.baseSnapshot(currMemdagHead, write)).meta.cookieJSON,
          FromInternalValueReason.PersistCompareCookies,
        ),
        fromInternalValue(
          perdagBaseSnapshot.meta.cookieJSON,
          FromInternalValueReason.PersistCompareCookies,
        ),
      ) > 0
    ) {
      console.log('skipping refresh because perdag snapshot is older');
      return undefined;
    }
    const newMutations = await localMutationsGreaterThan(
      currMemdagHead,
      {[clientID]: perdagLMID},
      write,
    );

    console.log('refreshing new mutations', newMutations.length);
    const newMemdagHead = await rebase(
      newMutations,
      perdagMainHead,
      write,
      mutators,
      lc,
    );

    const currCommit = await db.commitFromHash(currMemdagHead, write);
    const currMap = new btree.BTreeRead(write, currCommit.valueHash);
    const newCommit = await db.commitFromHash(newMemdagHead, write);
    const newMap = new btree.BTreeRead(
      write,
      (await db.commitFromHash(newMemdagHead, write)).valueHash,
    );
    const valueDiff = await btree.diff(currMap, newMap);
    const diffs: DiffsMap = new Map();
    if (valueDiff.length > 0) {
      diffs.set('', valueDiff);
    }
    await addDiffsForIndexes(currCommit, newCommit, write, diffs);
    console.log('refresh diffs', diffs);
    await write.setHead(db.DEFAULT_HEAD_NAME, newMemdagHead);
    await write.commit();
    return {newHead: newMemdagHead, diffs};
  });

  if (result === undefined) {
    return undefined;
  }
  // If this delete never happens, it's no big deal, some data will stay
  // alive longer but next refresh will move it.
  await perdag.withWrite(async write => {
    const clients = await getClients(write);
    const newClients = new Map(clients);
    const client = clients.get(clientID);
    assertNotUndefined(client); // should throw client not found
    const updatedClient = {
      ...client,
      headHash: perdagMainHead,
      tempRefreshHash: undefined,
    };
    newClients.set(clientID, updatedClient);
    await updateClientsDD31(newClients, write);
    await write.commit();
  });

  return result;
}
