import {EntryCache} from '../storage/entry-cache.js';
import {ReplicacheTransaction} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import {getClientRecord, putClientRecord} from '../types/client-record.js';
import {putVersion} from '../types/version.js';
import type {Version} from 'reflect-protocol';
import type {LogContext} from '@rocicorp/logger';
import type {PendingMutation} from '../types/mutation.js';

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
  pendingMutation: PendingMutation,
  mutators: MutatorMap,
  storage: Storage,
  version: Version,
): Promise<number | undefined> {
  const t0 = Date.now();
  try {
    lc.debug?.(
      'processing mutation',
      JSON.stringify(pendingMutation),
      'version',
      version,
    );
    const {clientID} = pendingMutation;
    const cache = new EntryCache(storage);
    const record = await getClientRecord(clientID, cache);
    if (!record) {
      lc.info?.('client not found', clientID);
      throw new Error(`Client ${clientID} not found`);
    }

    const expectedMutationID = record.lastMutationID + 1;
    if (pendingMutation.id < expectedMutationID) {
      lc.debug?.(
        'skipping duplicate mutation',
        JSON.stringify(pendingMutation),
      );
      return;
    }

    if (pendingMutation.id > expectedMutationID) {
      // This should never happen, the order is validated in the push message
      // handler.
      lc.error?.(
        'skipping out of order mutation',
        JSON.stringify(pendingMutation),
      );
      return;
    }

    const txCache = new EntryCache(storage);
    const tx = new ReplicacheTransaction(txCache, clientID, version);
    try {
      const mutator = mutators.get(pendingMutation.name);
      if (!mutator) {
        lc.info?.('skipping unknown mutator', JSON.stringify(pendingMutation));
      } else {
        await mutator(tx, pendingMutation.args);
        await txCache.flush();
      }
    } catch (e) {
      lc.info?.(
        'skipping mutation because error',
        JSON.stringify(pendingMutation),
        e,
      );
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
