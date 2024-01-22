import type {LogContext} from '@rocicorp/logger';
import type {Version} from 'reflect-protocol';
import type {Env} from 'reflect-shared/src/types.js';
import {timed} from 'shared/src/timed.js';
import {EntryCache} from '../storage/entry-cache.js';
import {ReplicacheTransaction} from '../storage/replicache-transaction.js';
import type {Storage} from '../storage/storage.js';
import {getClientRecord, putClientRecord} from '../types/client-record.js';
import type {PendingMutation} from '../types/mutation.js';
import {putVersion} from '../types/version.js';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Mutator = (tx: ReplicacheTransaction, args: any) => Promise<void>;
export type MutatorMap = Map<string, Mutator>;

// Runs a single mutation and updates storage accordingly.
// At exit:
// - storage will have been updated with effect of mutation
// - version key will have been updated if any change was made
// - client record of mutating client will have been updated
export function processMutation(
  lc: LogContext,
  env: Env,
  pendingMutation: PendingMutation,
  mutators: MutatorMap,
  storage: Storage,
  version: Version,
): Promise<number | undefined> {
  return timed(lc.debug, 'processMutation', () =>
    processMutationTimed(lc, env, pendingMutation, mutators, storage, version),
  );
}

async function processMutationTimed(
  lc: LogContext,
  env: Env,
  pendingMutation: PendingMutation,
  mutators: MutatorMap,
  storage: Storage,
  version: Version,
): Promise<number | undefined> {
  lc.debug?.(
    'processing mutation',
    describeMutation(pendingMutation),
    'version',
    version,
  );
  const {clientID} = pendingMutation;
  const cache = new EntryCache(storage);
  const record = await getClientRecord(clientID, cache);
  if (!record) {
    lc.error?.('client not found', clientID);
    throw new Error(`Client ${clientID} not found`);
  }

  const expectedMutationID = record.lastMutationID + 1;
  if (pendingMutation.id < expectedMutationID) {
    lc.debug?.(
      'skipping duplicate mutation',
      describeMutation(pendingMutation),
    );
    return;
  }

  if (pendingMutation.id > expectedMutationID) {
    // This should never happen, the order is validated in the push message
    // handler.
    lc.error?.(
      'skipping out of order mutation',
      describeMutation(pendingMutation),
    );
    return;
  }

  const txCache = new EntryCache(storage);
  const tx = new ReplicacheTransaction(
    txCache,
    clientID,
    pendingMutation.id,
    version,
    pendingMutation.auth,
    env,
  );
  try {
    const mutator = mutators.get(pendingMutation.name);
    if (!mutator) {
      lc.error?.('skipping unknown mutator', pendingMutation);
    } else {
      await mutator(tx, pendingMutation.args);
      await txCache.flush();
    }
  } catch (e) {
    lc.error?.('skipping mutation because of error', pendingMutation, e);
  }

  record.lastMutationID = expectedMutationID;
  record.lastMutationIDVersion = version;

  await putClientRecord(clientID, record, cache);
  await putVersion(version, cache);
  await cache.flush();
  return expectedMutationID;
}

function describeMutation(pendingMutation: PendingMutation) {
  return {
    clientID: pendingMutation.clientID,
    id: pendingMutation.id,
    name: pendingMutation.name,
  };
}
