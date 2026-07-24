/**
 * The `snapshot` API serves the purpose of:
 * - informing subscribers (i.e. view-syncers) of the (litestream)
 *   backup location from which to restore a replica snapshot
 * - checking whether a restored backup or existing replica is
 *   compatible with the change-streamer
 * - preventing change-log cleanup while a snapshot restore is in
 *   progress
 * - tracking the approximate time it takes from the beginning of
 *   snapshot "reservation" to the subsequent subscription, which
 *   serves as the minimum interval to wait before cleaning up
 *   backed up changes.
 */

import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {sleep} from '../../../../shared/src/sleep.ts';
import * as v from '../../../../shared/src/valita.ts';
import {type NormalizedZeroConfig} from '../../config/normalize.ts';
import {getShardConfig} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {ChangeStreamerHttpClient} from './change-streamer-http.ts';

const statusSchema = v.object({
  tag: v.literal('status'),

  /**
   * The location from which litestream should perform the restore.
   */
  backupURL: v.string(),

  /**
   * The `replicaVersion` of the backup. If a subscriber's restored or
   * existing replica is of a different version, it should delete it and
   * retry the restore from litestream (i.e. equivalent to a
   * `WrongReplicaVersion` response from a `/changes` subscription).
   */
  replicaVersion: v.string(),

  /**
   * The earliest watermark from which catchup is possible. If the
   * subscriber's replica is older that this watermark, it should delete it
   * and (retry the) restore from litestream (i.e. equivalent to a
   * `WatermarkTooOld` response from a `/changes` subscription).
   */
  minWatermark: v.string(),
});

export type SnapshotStatus = v.Infer<typeof statusSchema>;

const statusMessageSchema = v.tuple([v.literal('status'), statusSchema]);

export const snapshotMessageSchema = v.union(statusMessageSchema);

export type SnapshotMessage = v.Infer<typeof statusMessageSchema>;

export function reserveAndGetSnapshotStatus(
  lc: LogContext,
  config: NormalizedZeroConfig,
): Promise<SnapshotStatus> {
  const {promise: status, resolve, reject} = resolver<SnapshotStatus>();

  void (async function () {
    const abort = new AbortController();
    process.on('SIGINT', () => abort.abort());
    process.on('SIGTERM', () => abort.abort());

    for (let i = 0; ; i++) {
      let err: unknown;
      try {
        let resolved = false;
        const stream = await reserveSnapshot(lc, config);
        for await (const msg of stream) {
          // Capture the value of the status message that the change-streamer
          // backup monitor returns, and hold the connection open to
          // "reserve" the snapshot and prevent change log cleanup.
          resolve(msg[1]);
          resolved = true;
        }
        // The change-streamer itself closes the connection when the
        // subscription is started (or the reservation retried).
        if (resolved) {
          break;
        }
      } catch (e) {
        err = e;
      }
      // Retry in the view-syncer since it cannot proceed until it connects
      // to a (compatible) replication-manager. In particular, a
      // replication-manager that does not support the view-syncer's
      // change-streamer protocol will close the stream with an error; this
      // retry logic essentially delays the startup of a view-syncer until
      // a compatible replication-manager has been rolled out, allowing
      // replication-manager and view-syncer services to be updated in
      // parallel.
      lc.warn?.(
        `Unable to reserve snapshot (attempt ${i + 1}). Retrying in 5 seconds.`,
        String(err),
      );
      try {
        await sleep(5000, abort.signal);
      } catch (e) {
        return reject(e);
      }
    }
  })();

  return status;
}

function reserveSnapshot(
  lc: LogContext,
  config: NormalizedZeroConfig,
): Promise<Source<SnapshotMessage>> {
  const {taskID, change, changeStreamer} = config;
  const shardID = getShardConfig(config);

  const changeStreamerClient = new ChangeStreamerHttpClient(
    lc,
    shardID,
    change.db,
    changeStreamer.uri,
  );

  return changeStreamerClient.reserveSnapshot(taskID);
}
