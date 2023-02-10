import {
  readCommit,
  whenceHash,
  newWriteLocal,
  Commit,
  LocalMeta,
} from '../db/mod';
import type * as dag from '../dag/mod';
import type {Hash} from '../hash';
import type {MutatorDefs} from '../replicache';
import {WriteTransactionImpl} from '../transactions';
import {fromInternalValue, FromInternalValueReason} from '../internal-value';
import type {LogContext} from '@rocicorp/logger';
import type {ClientID} from './client-id';
import {isLocalMetaDD31} from '../db/commit';
import {assert} from '../asserts';

export async function rebaseMutation(
  mutation: Commit<LocalMeta>,
  dagWrite: dag.Write,
  basis: Hash,
  mutators: MutatorDefs,
  lc: LogContext,
  // TODO(greg): mutationClientID can be retrieved from mutation if LocalMeta
  // is a LocalMetaDD31.  As part of DD31 cleanup we can remove this arg.
  mutationClientID: ClientID,
  commitDagWrite = true,
): Promise<Hash> {
  const localMeta = mutation.meta;
  const name = localMeta.mutatorName;
  if (isLocalMetaDD31(localMeta)) {
    assert(
      localMeta.clientID === mutationClientID,
      'mutationClientID must match clientID of LocalMeta',
    );
  }
  const maybeMutatorImpl = mutators[name];
  if (!maybeMutatorImpl) {
    // Developers must not remove mutator names from code deployed with the
    // same schemaVersion because Replicache needs to be able to replay
    // mutations during pull.
    //
    // If we detect that this has happened, stub in a no-op mutator so that at
    // least sync can move forward. Note that the server-side mutation will
    // still get sent. This doesn't remove the queued local mutation, it just
    // removes its visible effects.
    lc.error?.(`Cannot rebase unknown mutator ${name}`);
  }
  const mutatorImpl =
    maybeMutatorImpl ||
    (async () => {
      // no op
    });

  const args = localMeta.mutatorArgsJSON;
  const jsonArgs = fromInternalValue(
    args,
    FromInternalValueReason.WriteTransactionMutateArgs,
  );

  const [, basisCommit] = await readCommit(whenceHash(basis), dagWrite);
  const nextMutationID = await basisCommit.getNextMutationID(
    mutationClientID,
    dagWrite,
  );
  if (nextMutationID !== localMeta.mutationID) {
    throw new Error(
      `Inconsistent mutation ID: original: ${localMeta.mutationID}, next: ${nextMutationID}`,
    );
  }

  const dbWrite = await newWriteLocal(
    whenceHash(basis),
    name,
    args,
    mutation.chunk.hash,
    dagWrite,
    localMeta.timestamp,
    mutationClientID,
  );

  const tx = new WriteTransactionImpl(mutationClientID, dbWrite, lc);
  await mutatorImpl(tx, jsonArgs);
  const [ref] = await tx.commit(false, commitDagWrite);
  return ref;
}
