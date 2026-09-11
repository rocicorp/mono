import {assert} from '../../../../shared/src/asserts.ts';
import {max, min} from '../../types/lexi-version.ts';
import type {Sink} from '../../types/streams.ts';
import type {
  ChangeSourceUpstream,
  ChangeStreamMessage,
} from '../change-source/protocol/current.ts';

type Opts = {
  trackPgChangeLog: boolean;
  trackBackup: boolean;
};

/**
 * Tracks the progress (watermark) of multiple streams:
 * - downstream transactions and status messages (the latter
 *   indicating LSNs that are not relevant to the publication
 *   but nevertheless need to be ACKed)
 * - PG change-log commits
 * - backup watermarks
 *
 * and sends upstream ACKs accordingly. When both the PG change-log
 * and backup watermarks are being considered for upstream ACKs
 * (i.e. RMv1.5), only watermarks that have been reached by both
 * stores are acked.
 *
 * The UpstreamAcker also takes into account that an upstream
 * connection can be disconnected and {@link reset()}. In this case,
 * the progress of the persistent stores is retained and the acks
 * are resent on the new upstream connection as necessary.
 */
export class UpstreamAcker {
  readonly #trackPgChangeLog: boolean;
  readonly #trackBackup: boolean;

  #pgChangeLogWatermark = '';
  #backupWatermark = '';

  #upstream: Sink<ChangeSourceUpstream> | undefined;
  #lastTx = '';
  #lastStatus = '';
  #lastAck = '';

  constructor({trackPgChangeLog, trackBackup}: Opts) {
    assert(
      trackPgChangeLog || trackBackup,
      `At least one of trackPgChangeLog or trackBackup must be true`,
    );
    this.#trackPgChangeLog = trackPgChangeLog;
    this.#trackBackup = trackBackup;
  }

  /**
   * Starts tracking a new upstream connection, which resumes the change stream
   * after `resumeWatermark`.
   *
   * Everything at or before `resumeWatermark` was committed on an earlier
   * connection, so it counts as committed on this one: a status watermark past
   * it waits for the tracked stores to reach it, as it would for a commit on
   * this connection. Otherwise the first keepalive of a stream that resumes
   * ahead of the stores -- a SQLite change log ahead of its backup, which is
   * the normal state -- would move the replication slot past transactions that
   * no store has persisted, and a task restored from the backup would then
   * resume below the slot, which the upstream moves forward without a word.
   *
   * Pass `''` when the stream resumes from what a tracked store has itself
   * persisted, which leaves nothing before it outstanding.
   */
  reset(upstream: Sink<ChangeSourceUpstream>, resumeWatermark: string) {
    this.#upstream = upstream;
    this.#lastTx = resumeWatermark;
    this.#lastStatus = '';
    this.#lastAck = '';
  }

  trackDownstream(downstream: ChangeStreamMessage) {
    const [tag, msg] = downstream;
    switch (tag) {
      case 'status':
        if (msg.ack) {
          this.#lastStatus = downstream[2].watermark;
        }
        this.#maybeAck();
        break;
      case 'commit':
        // A commit replayed from below the resume watermark does not lower it.
        this.#lastTx = max(this.#lastTx, downstream[2].watermark);
        this.#maybeAck();
        break;
    }
  }

  trackPgChangeLog(committedWatermark: string) {
    // Watermarks should never move backwards, but use max() defensively.
    this.#pgChangeLogWatermark = max(
      committedWatermark,
      this.#pgChangeLogWatermark,
    );
    this.#maybeAck();
  }

  trackBackup(backedUpWatermark: string) {
    // Watermarks should never move backwards, but use max() defensively.
    this.#backupWatermark = max(backedUpWatermark, this.#backupWatermark);
    this.#maybeAck();
  }

  #maybeAck() {
    const currentWatermark = !this.#trackBackup
      ? this.#pgChangeLogWatermark
      : !this.#trackPgChangeLog
        ? this.#backupWatermark
        : min(this.#pgChangeLogWatermark, this.#backupWatermark);
    if (currentWatermark > this.#lastAck) {
      this.#upstream?.push([
        'status',
        {tag: 'commit'},
        {watermark: currentWatermark},
      ]);
      this.#lastAck = currentWatermark;
    }
    // If all committed transactions have been acked, ack any outstanding
    // status LSN's thereafter (i.e. non-publication upstream changes).
    if (this.#lastAck >= this.#lastTx && this.#lastStatus > this.#lastAck) {
      this.#upstream?.push([
        'status',
        {ack: true},
        {watermark: this.#lastStatus},
      ]);
      this.#lastAck = this.#lastStatus;
    }
  }
}
