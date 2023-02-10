import type * as dag from '../dag/mod';
import type {Hash} from '../hash';
import type {MutatorDefs} from '../replicache';
import {WriteTransactionImpl} from '../transactions';
import {fromInternalValue, FromInternalValueReason} from '../internal-value';
import type {LogContext} from '@rocicorp/logger';
import type {ClientID} from '../sync/mod';
import {assert} from '../asserts';
import {Commit, fromHash, isLocalMetaDD31, LocalMetaSDD, Meta} from './commit';
import {newWriteLocal, Write} from './write';
import {whenceHash} from './read';

async function rebaseMutation(
  mutation: Commit<LocalMetaSDD>,
  dagWrite: dag.Write,
  basis: Hash,
  mutators: MutatorDefs,
  lc: LogContext,
  mutationClientID: ClientID,
): Promise<Write> {
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

  const basisCommit = await fromHash(basis, dagWrite);
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
  return dbWrite;
}

export async function rebaseMutationAndPutCommit(
  mutation: Commit<LocalMetaSDD>,
  dagWrite: dag.Write,
  basis: Hash,
  mutators: MutatorDefs,
  lc: LogContext,
  // TODO(greg): mutationClientID can be retrieved from mutation if LocalMeta
  // is a LocalMetaDD31.  As part of DD31 cleanup we can remove this arg.
  mutationClientID: ClientID,
): Promise<Commit<Meta>> {
  const tx = await rebaseMutation(
    mutation,
    dagWrite,
    basis,
    mutators,
    lc,
    mutationClientID,
  );
  return tx.putCommit();
}

export async function rebaseMutationAndCommit(
  mutation: Commit<LocalMetaSDD>,
  dagWrite: dag.Write,
  basis: Hash,
  headName: string,
  mutators: MutatorDefs,
  lc: LogContext,
  // TODO(greg): mutationClientID can be retrieved from mutation if LocalMeta
  // is a LocalMetaDD31.  As part of DD31 cleanup we can remove this arg.
  mutationClientID: ClientID,
): Promise<Hash> {
  const dbWrite = await rebaseMutation(
    mutation,
    dagWrite,
    basis,
    mutators,
    lc,
    mutationClientID,
  );
  return await dbWrite.commit(headName);
}
