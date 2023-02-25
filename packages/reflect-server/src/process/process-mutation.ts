import {EntryCache} from '../storage/entry-cache.js';
import {ReplicacheTransaction} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import {getClientRecord, putClientRecord} from '../types/client-record.js';
import {putVersion} from '../types/version.js';
import type {Version} from 'reflect-protocol';
import type {LogContext} from '@rocicorp/logger';
import type {Mutation} from 'reflect-protocol';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Mutator = (tx: ReplicacheTransaction, args: any) => Promise<void>;
export type MutatorMap = Map<string, Mutator>;

// Runs a single mutation and updates storage accordingly.
// At exit:
// - storage will have been updated with effect of mutation
// - version key will have been updated if any change was made
// - client record of mutating client will have been updated
export async function processMutation(
  lc: LogContext,
  mutation: Mutation,
  mutators: MutatorMap,
  storage: Storage,
  version: Version,
): Promise<number | undefined> {
  const t0 = Date.now();
  try {
    lc.debug?.(
      'processing mutation',
      JSON.stringify(mutation),
      'version',
      version,
    );
    const {clientID} = mutation;
    const cache = new EntryCache(storage);
    const record = await getClientRecord(clientID, cache);
    if (!record) {
      lc.info?.('client not found', clientID);
      throw new Error(`Client ${clientID} not found`);
    }

    const expectedMutationID = record.lastMutationID + 1;
    if (mutation.id < expectedMutationID) {
      lc.debug?.('skipping duplicate mutation', JSON.stringify(mutation));
      return;
    }

    if (mutation.id > expectedMutationID) {
      lc.info?.('skipping out of order mutation', JSON.stringify(mutation));
      return;
    }

    const tx = new ReplicacheTransaction(cache, clientID, version);
    try {
      const mutator = mutators.get(mutation.name);
      if (!mutator) {
        lc.info?.('skipping unknown mutator', JSON.stringify(mutation));
      } else {
        await mutator(tx, mutation.args);
      }
    } catch (e) {
      lc.info?.('skipping mutation because error', JSON.stringify(mutation), e);
    }

    record.lastMutationID = expectedMutationID;
    record.lastMutationIDVersion = version;

    await putClientRecord(clientID, record, cache);
    await putVersion(version, cache);
    await cache.flush();
    return expectedMutationID;
  } finally {
    lc.debug?.(`processMutation took ${Date.now() - t0} ms`);
  }
}
