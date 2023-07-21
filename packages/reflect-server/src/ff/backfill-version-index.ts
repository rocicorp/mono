import type {LogContext} from '@rocicorp/logger';
import type {DurableStorage} from '../storage/durable-storage.js';
import type {Version} from 'reflect-protocol';
import {
  decodeUserValueVersionKey,
  userValuePrefix,
  userValueSchema,
  userValueVersionEntry,
  userValueVersionIndexPrefix,
  userValueVersionInfoSchema,
  userValueVersionKey,
} from '../types/user-value.js';

/**
 * Backfills the version index at "v/" which allows Fast(er) Forward to efficiently
 * compute the changes since a given version (without reading all objects).
 *
 * Because the server can be rolled back and forth between versions that maintain
 * and do not maintain the version index, there are three possibilities to account for:
 *
 * 1. An object does not have an entry in the version index.
 * 2. An object has an outdated entry in the version index.
 *    (e.g. server rolled back, new objects written, server rolled forward)
 * 3. An object has a correct entry in the version index.
 *
 * To handle all cases, the algorithm is as follows:
 *
 * 1. Load the version index as a mapping from object key to indexed version.
 *    Note that we assume that this fits into memory, as it is only keys and versions,
 *    and not object values.
 * 2. Scan the objects in batches, and check each version against the index entry.
 *    Because this loads object values, it is done as an incremental scan to avoid
 *    memory exhaustion.
 * 3. Update the version index as necessary, filling in missing entries and correcting
 *    outdated entries.
 * 4. Flush version index updates per batch so that a migration makes progress even
 *    if it is aborted before completing.
 */
export async function backfillVersionIndex(
  log: LogContext,
  storage: DurableStorage,
): Promise<void> {
  const indexedObjectVersions = new Map<string, Version>();
  for await (const indexEntry of storage.scan(
    {prefix: userValueVersionIndexPrefix},
    userValueVersionInfoSchema,
  )) {
    const decoded = decodeUserValueVersionKey(indexEntry[0]);
    indexedObjectVersions.set(decoded.userKey, decoded.version);
  }
  log.info?.(`Loaded ${indexedObjectVersions.size} version index entries`);

  let totalIndexUpdates = 0;
  let totalObjects = 0;

  for await (const userValues of storage.batchScan(
    {prefix: userValuePrefix},
    userValueSchema,
    64, // batchSize to keep loaded values within the DO's memory constraints.
  )) {
    let indexUpdates = 0;
    for (const [key, value] of userValues.entries()) {
      totalObjects++;

      const userKey = key.substring(userValuePrefix.length);
      const indexedVersion = indexedObjectVersions.get(userKey);
      if (indexedVersion === value.version) {
        continue; // Index entry matches.
      }
      if (indexedVersion !== undefined) {
        void storage.del(userValueVersionKey(userKey, indexedVersion));
      }
      const indexEntry = userValueVersionEntry(userKey, value);
      void storage.put(indexEntry.key, indexEntry.value);
      indexUpdates++;
    }
    if (indexUpdates > 0) {
      log.info?.(`Flushing ${indexUpdates} updates to version index`);
      await storage.flush();
      totalIndexUpdates += indexUpdates;
    }
  }
  log.info?.(
    `Updated ${totalIndexUpdates} index entries out of ${totalObjects} objects`,
  );
}
