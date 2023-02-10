import {expect} from '@esm-bundle/chai';
import type {Chain} from '../db/test-helpers.js';
import type * as dag from '../dag/mod.js';
import * as db from '../db/mod.js';
import * as sync from '../sync/mod.js';
import {commitIsSnapshot} from '../db/commit.js';

// See db.test_helpers for addLocal, addSnapshot, etc. We can't put addLocalRebase
// there because sync depends on db, and addLocalRebase depends on sync.

// addSyncSnapshot adds a sync snapshot off of the main chain's base snapshot and
// returns it (in chain order). Caller needs to supply which commit to take indexes
// from because it is context dependent (they should come from the parent of the
// first commit to rebase, or from head if no commits will be rebased).

export async function addSyncSnapshot(
  chain: Chain,
  store: dag.Store,
  takeIndexesFrom: number,
  clientID: sync.ClientID,
  dd31: boolean,
): Promise<Chain> {
  expect(chain.length >= 2).to.be.true;

  let maybeBaseSnapshot: db.Commit<db.SnapshotMeta> | undefined;
  for (let i = chain.length - 1; i > 0; i--) {
    const commit = chain[i - 1];
    if (commitIsSnapshot(commit)) {
      maybeBaseSnapshot = commit;
      break;
    }
  }
  if (maybeBaseSnapshot === undefined) {
    throw new Error("main chain doesn't have a snapshot or local commit");
  }
  const baseSnapshot = maybeBaseSnapshot;
  const syncChain: Chain = [];

  // Add sync snapshot.
  const cookie = `sync_cookie_${chain.length}`;
  await store.withWrite(async dagWrite => {
    if (dd31) {
      const w = await db.newWriteSnapshotDD31(
        db.whenceHash(baseSnapshot.chunk.hash),
        {[clientID]: await baseSnapshot.getMutationID(clientID, dagWrite)},
        cookie,
        dagWrite,
        clientID,
      );
      await w.commit(sync.SYNC_HEAD_NAME);
    } else {
      const indexes = db.readIndexesForWrite(chain[takeIndexesFrom], dagWrite);
      const w = await db.newWriteSnapshotSDD(
        db.whenceHash(baseSnapshot.chunk.hash),
        await baseSnapshot.getMutationID(clientID, dagWrite),
        cookie,
        dagWrite,
        indexes,
        clientID,
      );
      await w.commit(sync.SYNC_HEAD_NAME);
    }
  });
  const [, commit] = await store.withRead(async dagRead => {
    return await db.readCommit(db.whenceHead(sync.SYNC_HEAD_NAME), dagRead);
  });
  syncChain.push(commit);

  return syncChain;
}
