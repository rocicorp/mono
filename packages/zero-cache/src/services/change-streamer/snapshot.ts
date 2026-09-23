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
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {type NormalizedZeroConfig} from '../../config/normalize.ts';
import {getShardConfig} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {ChangeStreamerHttpClient} from './change-streamer-http.ts';

// The schema definitions live in the pure schema module (with no dependency on
// the HTTP client) so that subscribe.ts can import them without forming an
// import cycle through change-streamer-http.ts. Imported here for local use and
// re-exported for existing consumers.
import {
  snapshotMessageSchema,
  statusSchema,
  type SnapshotMessage,
  type SnapshotStatus,
} from './snapshot-message.ts';

export {snapshotMessageSchema, statusSchema};
export type {SnapshotMessage, SnapshotStatus};

export type ReserveSnapshot = (
  lc: LogContext,
  config: NormalizedZeroConfig,
) => Promise<Source<SnapshotMessage>>;

export function reserveAndGetSnapshotStatus(
  lc: LogContext,
  config: NormalizedZeroConfig,
  reserve: ReserveSnapshot = reserveSnapshot, // for testing
): Promise<SnapshotStatus> {
  const {promise: status, resolve, reject} = resolver<SnapshotStatus>();

  void (async function () {
    const abort = new AbortController();
    const {signal} = abort;
    const onSignal = () => abort.abort();
    process.on('SIGINT', onSignal);
    process.on('SIGTERM', onSignal);

    try {
      for (let i = 0; ; i++) {
        let err: unknown;
        try {
          let resolved = false;
          const stream = await untilAborted(reserve(lc, config), signal);
          // A signal while the stream is open cancels it, which ends the
          // iteration below with an AbortError.
          const cancelStream = () => stream.cancel(new AbortError('Aborted'));
          signal.addEventListener('abort', cancelStream, {once: true});
          try {
            for await (const msg of stream) {
              // Capture the value of the status message that the change-streamer
              // backup monitor returns, and hold the connection open to
              // "reserve" the snapshot and prevent change log cleanup.
              resolve(msg[1]);
              resolved = true;
            }
          } finally {
            signal.removeEventListener('abort', cancelStream);
          }
          // The change-streamer itself closes the connection when the
          // subscription is started (or the reservation retried).
          if (resolved) {
            break;
          }
        } catch (e) {
          err = e;
        }
        if (signal.aborted) {
          // (A no-op if the status was already resolved.)
          return reject(
            err instanceof AbortError ? err : new AbortError('Aborted'),
          );
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
    } finally {
      // This function is called repeatedly (e.g. by the replicator's restore
      // loop), so the signal handlers must not outlive the reservation.
      process.off('SIGINT', onSignal);
      process.off('SIGTERM', onSignal);
    }
  })();

  return status;
}

/**
 * Resolves with the reserved stream, or rejects with an AbortError if the
 * `signal` is aborted while the reservation is pending (in which case a
 * stream that arrives later is canceled rather than held open).
 */
function untilAborted<T>(
  reservation: Promise<Source<T>>,
  signal: AbortSignal,
): Promise<Source<T>> {
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(new AbortError('Aborted'));
    signal.addEventListener('abort', onAbort, {once: true});
    reservation.then(
      stream => {
        signal.removeEventListener('abort', onAbort);
        if (signal.aborted) {
          stream.cancel();
        } else {
          resolve(stream);
        }
      },
      e => {
        signal.removeEventListener('abort', onAbort);
        reject(e);
      },
    );
  });
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
