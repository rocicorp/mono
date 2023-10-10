import {expect} from 'chai';
import type {Store} from '../dag/store.js';
import {
  Commit,
  SnapshotMeta,
  commitFromHead,
  commitIsSnapshot,
} from '../db/commit.js';
import type {Chain} from '../db/test-helpers.js';
import {
  newWriteSnapshotDD31,
  newWriteSnapshotSDD,
  readIndexesForWrite,
} from '../db/write.js';
import {FormatVersion} from '../format-version.js';
import {SYNC_HEAD_NAME} from '../sync/sync-head-name.js';
import {withRead, withWrite} from '../with-transactions.js';
import type {ClientID} from './ids.js';

// See db.test_helpers for addLocal, addSnapshot, etc. We can't put addLocalRebase
// there because sync depends on db, and addLocalRebase depends on sync.

// addSyncSnapshot adds a sync snapshot off of the main chain's base snapshot and
// returns it (in chain order). Caller needs to supply which commit to take indexes
// from because it is context dependent (they should come from the parent of the
// first commit to rebase, or from head if no commits will be rebased).

export async function addSyncSnapshot(
  chain: Chain,
  store: Store,
  takeIndexesFrom: number,
  clientID: ClientID,
  formatVersion: FormatVersion,
): Promise<Chain> {
  expect(chain.length >= 2).to.be.true;

  let maybeBaseSnapshot: Commit<SnapshotMeta> | undefined;
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
  await withWrite(store, async dagWrite => {
    if (formatVersion >= FormatVersion.DD31) {
      const w = await newWriteSnapshotDD31(
        baseSnapshot.chunk.hash,
        {[clientID]: await baseSnapshot.getMutationID(clientID, dagWrite)},
        cookie,
        dagWrite,
        clientID,
        formatVersion,
      );
      await w.commit(SYNC_HEAD_NAME);
    } else {
      const indexes = readIndexesForWrite(
        chain[takeIndexesFrom],
        dagWrite,
        formatVersion,
      );
      const w = await newWriteSnapshotSDD(
        baseSnapshot.chunk.hash,
        await baseSnapshot.getMutationID(clientID, dagWrite),
        cookie,
        dagWrite,
        indexes,
        clientID,
        formatVersion,
      );
      await w.commit(SYNC_HEAD_NAME);
    }
  });
  const commit = await withRead(store, dagRead =>
    commitFromHead(SYNC_HEAD_NAME, dagRead),
  );
  syncChain.push(commit);

  return syncChain;
}
