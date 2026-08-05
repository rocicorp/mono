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

  reset(upstream: Sink<ChangeSourceUpstream>) {
    this.#upstream = upstream;
    this.#lastTx = '';
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
        this.#lastTx = downstream[2].watermark;
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
