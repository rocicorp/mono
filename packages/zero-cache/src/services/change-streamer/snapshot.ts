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
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {promiseOrAbort} from '../../../../shared/src/promise-race.ts';
import {sleep} from '../../../../shared/src/sleep.ts';

// The schema definitions live in the pure schema module (with no dependency on
// the HTTP client) so that subscribe.ts can import them without forming an
// import cycle through change-streamer-http.ts. Imported here for local use and
// re-exported for existing consumers.
import type {
  ReservationFollowup,
  SnapshotReserver,
} from './change-streamer-http.ts';
import type {SnapshotStatus} from './subscribe.ts';

export type {SnapshotStatus};

export async function reserveAndGetSnapshotStatus(
  lc: LogContext,
  taskID: string,
  changeStreamer: SnapshotReserver,
): Promise<{reserved: SnapshotStatus; followup: ReservationFollowup}> {
  const abort = new AbortController();
  const {signal} = abort;
  // Use our own AbortError as the abort reason (rather than the native
  // DOMException that AbortController.abort() defaults to) so that
  // promiseOrAbort()'s rejection, which propagates `signal.reason` verbatim,
  // is always an instance of the shared AbortError type.
  const onSignal = () => abort.abort(new AbortError('SIGTERM/SIGINT'));
  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);

  try {
    for (let i = 0; ; i++) {
      let err: unknown;
      const reservation = changeStreamer.reserveSnapshot(taskID);
      try {
        return await promiseOrAbort(reservation, signal);
      } catch (e) {
        err = e;
      }
      if (signal.aborted) {
        // promiseOrAbort() does not cancel the loser of the race: the
        // reservation may still resolve later. If it does, cancel its
        // connection rather than leaking it, since this function has already
        // given up and nobody will call followup.subscribe().
        reservation.then(
          ({followup}) => followup.cancel(),
          () => {},
        );
        throw err;
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
      await sleep(5000, signal);
    }
  } finally {
    // This function is called repeatedly (e.g. by the replicator's restore
    // loop), so the signal handlers must not outlive the reservation.
    process.off('SIGINT', onSignal);
    process.off('SIGTERM', onSignal);
  }
}
