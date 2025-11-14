import {expect} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import type {Store} from '../dag/store.ts';
import type {Commit} from '../db/commit.ts';
import {
  type SnapshotMeta,
  commitFromHead,
  commitIsSnapshot,
} from '../db/commit.ts';
import type {Chain} from '../db/test-helpers.ts';
import {newWriteSnapshotDD31} from '../db/write.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.ts';
import type {ClientID} from './ids.ts';
import {SYNC_HEAD_NAME} from './sync-head-name.ts';

type FormatVersion = (typeof FormatVersion)[keyof typeof FormatVersion];

// See db.test_helpers for addLocal, addSnapshot, etc. We can't put addLocalRebase
// there because sync depends on db, and addLocalRebase depends on sync.

// addSyncSnapshot adds a sync snapshot off of the main chain's base snapshot and
// returns it (in chain order). Caller needs to supply which commit to take indexes
// from because it is context dependent (they should come from the parent of the
// first commit to rebase, or from head if no commits will be rebased).

export async function addSyncSnapshot(
  chain: Chain,
  store: Store,
  _takeIndexesFrom: number,
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
  await withWriteNoImplicitCommit(store, async dagWrite => {
    assert(formatVersion >= FormatVersion.DD31);
    const w = await newWriteSnapshotDD31(
      baseSnapshot.chunk.hash,
      {[clientID]: await baseSnapshot.getMutationID(clientID, dagWrite)},
      cookie,
      dagWrite,
      clientID,
      formatVersion,
    );
    await w.commit(SYNC_HEAD_NAME);
  });
  const commit = await withRead(store, dagRead =>
    commitFromHead(SYNC_HEAD_NAME, dagRead),
  );
  syncChain.push(commit);

  return syncChain;
}
