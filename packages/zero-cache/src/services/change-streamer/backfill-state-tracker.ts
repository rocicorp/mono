import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../../shared/src/must.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {Subscription} from '../../types/subscription.ts';
import {
  getSuperset,
  withResumePoint,
} from '../change-source/protocol/backfill-progress.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {BackfillRequest} from '../change-source/protocol/current/upstream.ts';
import {BackfillState} from './backfill-state.ts';
import type {PreSerializedBatch} from './broadcast.ts';
import type {SubscriberContext} from './change-streamer.ts';
import type {Subscriber, SubscriberOptions} from './subscriber.ts';

export type SubscriberBackfillOptions = Pick<
  SubscriberOptions,
  'backfills' | 'onAligned' | 'onBackfillIgnored'
>;

/**
 * Tracks the pending backfills (and their progress) of the change log, the
 * subscribers, and the current change stream, in order to determine:
 *
 * * the BackfillRequests with which to start a change stream, i.e. the
 *   superset of the pending backfills of the change log and all aligned
 *   subscribers, starting from the earliest progress, and
 * * whether (and why) the change stream must be restarted, i.e. when a
 *   newly aligned subscriber's pending backfills are not covered by the
 *   current stream.
 *
 * The change-streamer calls {@link startStream()} whenever it starts a change
 * stream, {@link track()} for every change that it forwards, and restarts
 * the stream at the next transaction boundary whenever a
 * {@link restartReason} is set.
 */
export class BackfillStateTracker {
  readonly #lc: LogContext;

  /**
   * The pending backfills of the change log itself, i.e. those of a
   * (hypothetical) subscriber that has applied every change in the log.
   * Whenever a stream is started, it is reset from the change log's cookie
   * jar (which is consistent with the stream's starting watermark), carrying
   * over the progress tracked in memory (as the cookie jar is
   * progress-agnostic). It stands in for subscribers that do not report their
   * pending backfills (i.e. protocol < v8), and ensures that backfills that
   * are pending in the change log continue to be requested.
   */
  #logBackfills = new BackfillState();

  /**
   * The backfill progress of the current change stream (i.e. "session"),
   * seeded from the BackfillRequests with which the stream was started.
   * This determines whether a (newly aligned) subscriber's pending backfills
   * are covered by the stream.
   */
  #sessionBackfills = new BackfillState();

  /** Subscribers that report their pending backfills (protocol v8+). */
  readonly #subscribers = new Set<Subscriber>();

  /**
   * Set (to the reason) when the change stream must be restarted with
   * new BackfillRequests.
   */
  #restartReason: string | null = null;

  readonly #restarts = getOrCreateCounter(
    'replication',
    'backfill_stream_restarts',
    'Count of change stream restarts initiated to rewind backfills for ' +
      'subscribers whose pending backfills were not covered by the stream',
  );
  readonly #unexpectedIgnores = getOrCreateCounter(
    'replication',
    'backfill_unexpected_ignores',
    'Count of backfill messages unexpectedly ignored (for some columns) by ' +
      'an aligned subscriber. The expected count is 0.',
  );

  constructor(lc: LogContext) {
    this.#lc = lc;
  }

  /**
   * The reason for which the change stream must be restarted, or `null` if
   * it need not be. The restart should happen at the next transaction
   * boundary, i.e. upon the next `commit`, or `status` message outside of a
   * transaction.
   */
  get restartReason(): string | null {
    return this.#restartReason;
  }

  /**
   * Computes the BackfillRequests with which to start a new change stream:
   * the superset of the pending backfills of the change log and of all
   * aligned subscribers, all of which are consistent with the stream's
   * starting watermark since streams are (re)started at transaction
   * boundaries.
   *
   * @param logBackfillRequests The BackfillRequests from the change log's
   *        cookie jar, consistent with the stream's starting watermark.
   */
  startStream(logBackfillRequests: BackfillRequest[]): BackfillRequest[] {
    // The cookie jar can be behind #logBackfills (e.g. if the storer failed
    // to persist a forwarded commit), in which case the new stream
    // re-delivers those changes.
    this.#logBackfills = new BackfillState(
      this.#logBackfills.withProgress(logBackfillRequests),
    );

    const aligned = [...this.#subscribers].filter(s => s.aligned);
    const requests = getSuperset(
      this.#logBackfills.requests(),
      ...aligned.map(s => must(s.backfills).requests()),
    ).map(withResumePoint);

    this.#sessionBackfills = new BackfillState(requests);
    // Aligned subscribers are accounted for in the requests.
    this.#restartReason = null;
    if (requests.length) {
      this.#lc.info?.(
        `starting change stream with ${requests.length} backfill requests ` +
          `(from the change log and ${aligned.length} subscribers)`,
        {requests},
      );
    }
    return requests;
  }

  /**
   * Tracks a change that is forwarded to subscribers. This must be called
   * (synchronously) before the change is forwarded, so that the stream's
   * state is never ahead of that of aligned subscribers.
   */
  track(change: ChangeStreamData) {
    this.#sessionBackfills.apply(change);
    this.#logBackfills.apply(change);
  }

  /** Logs and records a (planned) restart for the {@link restartReason}. */
  recordRestart() {
    this.#lc.info?.(`restarting change stream: ${this.#restartReason}`);
    this.#restarts.add(1);
  }

  /**
   * Returns the backfill-related {@link SubscriberOptions} for a new
   * subscriber, which must subsequently be passed to {@link register()}.
   */
  subscriberOptions(
    lc: LogContext,
    ctx: SubscriberContext,
  ): SubscriberBackfillOptions {
    const backfills = newBackfillState(lc, ctx);
    if (!backfills) {
      return {};
    }
    return {
      backfills,
      onAligned: subscriber => {
        if (this.#subscribers.has(subscriber)) {
          this.#checkCoverage(subscriber);
        }
      },
      onBackfillIgnored: columns => {
        if (this.#restartReason === null) {
          lc.warn?.(
            `aligned subscriber ignored backfill data for ${columns.join(', ')}`,
          );
          this.#unexpectedIgnores.add(1);
        }
      },
    };
  }

  /**
   * Registers a subscriber (created with {@link subscriberOptions()}) until
   * its `downstream` is closed. This is a no-op for subscribers that do not
   * report their pending backfills.
   */
  register(
    subscriber: Subscriber,
    downstream: Subscription<string | PreSerializedBatch>,
  ) {
    if (subscriber.backfills) {
      this.#subscribers.add(subscriber);
      downstream.addCloseHandler(() => this.#subscribers.delete(subscriber));
    }
  }

  /**
   * Checks whether the pending backfills of a newly aligned subscriber are
   * covered by the current stream, requesting a restart of the stream if not.
   *
   * This can be checked as soon as the subscriber is aligned (even in the
   * middle of a transaction), since both the subscriber's and the stream's
   * BackfillStates compare their committed state, and the stream's state is
   * never updated with a change before the change is forwarded to (aligned)
   * subscribers. The restart itself happens at the next transaction boundary.
   */
  #checkCoverage(subscriber: Subscriber) {
    const uncovered = must(subscriber.backfills).uncovered(
      this.#sessionBackfills,
    );
    if (uncovered.length) {
      this.#restartReason ??=
        `subscriber ${subscriber.id} needs backfills that are not ` +
        `covered by the current stream: ${uncovered.join(', ')}`;
    }
  }
}

function newBackfillState(
  lc: LogContext,
  ctx: SubscriberContext,
): BackfillState | undefined {
  if (ctx.backfills === undefined) {
    return undefined; // protocol < v8
  }
  try {
    return new BackfillState(ctx.backfills);
  } catch (e) {
    lc.warn?.(`invalid backfills from subscriber ${ctx.id}`, e);
    return undefined;
  }
}
