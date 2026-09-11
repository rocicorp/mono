import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import type {Write as DagWrite} from '../dag/store.ts';
import * as FormatVersion from '../format-version-enum.ts';
import type {Hash} from '../hash.ts';
import type {ZeroTxData} from '../replicache-options.ts';
import type {ClientID} from '../sync/ids.ts';
import {WriteTransactionImpl} from '../transactions.ts';
import type {MutatorDefs} from '../types.ts';
import {
  type Commit,
  type LocalMeta,
  type LocalMetaDD31,
  type Meta,
  assertLocalMetaDD31,
  commitFromHash,
  isLocalMetaDD31,
} from './commit.ts';
import type {Write} from './write.ts';
import {newWriteLocal} from './write.ts';

type FormatVersion = Enum<typeof FormatVersion>;

/**
 * The result of replaying one mutation.
 *
 * `zeroData` is the Zero transaction data to use for the next mutation in the
 * same replay. Each mutation runs against its own fork. On success that fork is
 * returned, and if the mutator throws the caller gets back the one it passed
 * in, without the failed mutation's IVM writes. See `rebaseMutation`.
 */
export type RebaseResult<T> = {
  result: T;
  zeroData: ZeroTxData | undefined;
};

async function rebaseMutation(
  mutation: Commit<LocalMetaDD31>,
  dagWrite: DagWrite,
  basisHash: Hash,
  mutators: MutatorDefs,
  lc: LogContext,
  mutationClientID: ClientID,
  formatVersion: FormatVersion,
  zeroData: ZeroTxData | undefined,
): Promise<RebaseResult<Write>> {
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

  const basisCommit = await commitFromHash(basisHash, dagWrite);
  const nextMutationID = await basisCommit.getNextMutationID(
    mutationClientID,
    dagWrite,
  );
  if (nextMutationID !== localMeta.mutationID) {
    throw new Error(
      `Inconsistent mutation ID: original: ${localMeta.mutationID}, next: ${nextMutationID} - mutationClientID: ${mutationClientID} mutatorName: ${name}`,
    );
  }

  if (formatVersion >= FormatVersion.DD31) {
    assertLocalMetaDD31(localMeta);
  }

  const newWrite = () =>
    newWriteLocal(
      basisHash,
      name,
      args,
      mutation.chunk.hash,
      dagWrite,
      localMeta.timestamp,
      mutationClientID,
      formatVersion,
    );

  const dbWrite = await newWrite();

  // Run the mutator against a fork so that if it throws, its IVM writes are
  // discarded along with its `Write`.
  const txData = zeroData?.fork();

  const tx = new WriteTransactionImpl(
    mutationClientID,
    await dbWrite.getMutationID(),
    'rebase',
    txData,
    dbWrite,
    lc,
  );

  try {
    await mutatorImpl(tx, args);
  } catch (e) {
    // A mutator can throw here without anything being wrong: it is being run
    // again against a newer server snapshot, so a check that passed when the
    // user first ran it can fail now. Rethrowing fails the whole rebase, which
    // in Zero fails the poke and disconnects the client. The mutation stays
    // pending, so the next poke fails the same way.
    //
    // Only the local prediction is dropped. The mutation is still pending and
    // is still sent to the server, which decides whether it applied.
    //
    // `dbWrite` is dropped rather than closed: it shares `dagWrite` with the
    // caller. Dropping it is enough because a `Write` does not modify anything
    // outside itself and only writes chunks when it is committed.
    //
    // Engine errors raised inside the mutator are retired the same way, rather
    // than being sorted out here. A client whose state was garbage collected
    // throws ChunkNotFoundError from its reads, and that is detected on its
    // next mutation, where replicache-impl converts it to a
    // ClientStateNotFoundError and calls onClientStateNotFound.
    lc.info?.(`Rebase of mutator ${name} threw, abandoning its prediction`, e);
    return {result: await newWrite(), zeroData};
  }

  return {result: dbWrite, zeroData: txData};
}

export async function rebaseMutationAndPutCommit(
  mutation: Commit<LocalMeta>,
  dagWrite: DagWrite,
  basis: Hash,
  mutators: MutatorDefs,
  lc: LogContext,
  // TODO(greg): mutationClientID can be retrieved from mutation if LocalMeta
  // is a LocalMetaDD31.  As part of DD31 cleanup we can remove this arg.
  mutationClientID: ClientID,
  formatVersion: FormatVersion,
  zeroData: ZeroTxData | undefined,
): Promise<RebaseResult<Commit<Meta>>> {
  const {result: tx, zeroData: next} = await rebaseMutation(
    mutation,
    dagWrite,
    basis,
    mutators,
    lc,
    mutationClientID,
    formatVersion,
    zeroData,
  );
  return {result: await tx.putCommit(), zeroData: next};
}

export async function rebaseMutationAndCommit(
  mutation: Commit<LocalMeta>,
  dagWrite: DagWrite,
  basis: Hash,
  headName: string,
  mutators: MutatorDefs,
  lc: LogContext,
  // TODO(greg): mutationClientID can be retrieved from mutation if LocalMeta
  // is a LocalMetaDD31.  As part of DD31 cleanup we can remove this arg.
  mutationClientID: ClientID,
  formatVersion: FormatVersion,
  zeroData: ZeroTxData | undefined,
): Promise<RebaseResult<Hash>> {
  const {result: dbWrite, zeroData: next} = await rebaseMutation(
    mutation,
    dagWrite,
    basis,
    mutators,
    lc,
    mutationClientID,
    formatVersion,
    zeroData,
  );
  return {result: await dbWrite.commit(headName), zeroData: next};
}
