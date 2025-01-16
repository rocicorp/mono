import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.js';
import type {Enum} from '../../../shared/src/enum.js';
import type {Write as DagWrite} from '../dag/store.js';
import * as FormatVersion from '../format-version-enum.js';
import type {Hash} from '../hash.js';
import type {ClientID} from '../sync/ids.js';
import {WriteTransactionImpl} from '../transactions.js';
import type {MutatorDefs} from '../types.js';
import {
  Commit,
  type LocalMeta,
  type LocalMetaDD31,
  type Meta,
  assertLocalMetaDD31,
  commitFromHash,
  isLocalMetaDD31,
} from './commit.js';
import {Write, newWriteLocal} from './write.js';

type FormatVersion = Enum<typeof FormatVersion>;

async function rebaseMutation(
  mutation: Commit<LocalMetaDD31>,
  dagWrite: DagWrite,
  basisHash: Hash,
  mutators: MutatorDefs,
  lc: LogContext,
  mutationClientID: ClientID,
  formatVersion: FormatVersion,
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

  const dbWrite = await newWriteLocal(
    basisHash,
    name,
    args,
    mutation.chunk.hash,
    dagWrite,
    localMeta.timestamp,
    mutationClientID,
    formatVersion,
  );

  const tx = new WriteTransactionImpl(
    mutationClientID,
    await dbWrite.getMutationID(),
    'rebase',
    dbWrite,
    lc,
  );
  await mutatorImpl(tx, args);
  return dbWrite;
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
): Promise<Commit<Meta>> {
  const tx = await rebaseMutation(
    mutation,
    dagWrite,
    basis,
    mutators,
    lc,
    mutationClientID,
    formatVersion,
  );
  return tx.putCommit();
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
): Promise<Hash> {
  const dbWrite = await rebaseMutation(
    mutation,
    dagWrite,
    basis,
    mutators,
    lc,
    mutationClientID,
    formatVersion,
  );
  return dbWrite.commit(headName);
}
