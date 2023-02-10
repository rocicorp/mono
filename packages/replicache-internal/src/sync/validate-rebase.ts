import type * as dag from '../dag/mod';
import * as db from '../db/mod';
import * as sync from './mod';
import type {Hash} from '../hash';
import type {InternalValue} from '../internal-value.js';

export type RebaseOpts = {
  basis: Hash;
  original: Hash;
};

export async function validateRebase(
  opts: RebaseOpts,
  dagRead: dag.Read,
  mutatorName: string,
  _args: InternalValue | undefined,
): Promise<void> {
  // Ensure the rebase commit is going on top of the current sync head.
  const syncHeadHash = await dagRead.getHead(sync.SYNC_HEAD_NAME);
  if (syncHeadHash !== opts.basis) {
    throw new Error(
      `WrongSyncHeadJSLogInfo: sync head is ${syncHeadHash}, transaction basis is ${opts.basis}`,
    );
  }

  // Ensure rebase and original commit mutator names match.
  const [, original] = await db.readCommit(
    db.whenceHash(opts.original),
    dagRead,
  );
  if (original.isLocal()) {
    const lm = original.meta;
    if (lm.mutatorName !== mutatorName) {
      throw new Error(
        `Inconsistent mutator: original: ${lm.mutatorName}, request: ${mutatorName}`,
      );
    }
  } else {
    throw new Error('Internal programmer error: Commit is not a local commit');
  }

  // Ensure rebase and original commit mutation ids names match.
  const [, basis] = await db.readCommit(db.whenceHash(opts.basis), dagRead);
  if (basis.nextMutationID !== original.mutationID) {
    throw new Error(
      `Inconsistent mutation ID: original: ${original.mutationID}, next: ${basis.nextMutationID}`,
    );
  }

  // TODO: temporarily skipping check that args are the same.
  // https://github.com/rocicorp/repc/issues/151
}
