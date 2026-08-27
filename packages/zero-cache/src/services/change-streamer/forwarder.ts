import type {LogContext} from '@rocicorp/logger';
import {joinIterables, wrapIterable} from '../../../../shared/src/iterables.ts';
import {
  getOrCreateCounter,
  getOrCreateGauge,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import {Broadcast, type EarlyReleaseOptions} from './broadcast.ts';
import type {ChangeTag, Status, WatermarkedChange} from './change-streamer.ts';
import {StreamTooFarBehind} from './error-type-enum.ts';
import type {Subscriber, SubscriberStats} from './subscriber.ts';

export type ProgressMonitorOptions = {
  flowControlConsensusTimeoutProportion: number;
  flowControlSlowSubscriberGracePeriodMs?: number | undefined;
  // Injectable timers, for deterministic testing.
  setTimeoutFn?: typeof setTimeout | undefined;
  clearTimeoutFn?: typeof clearTimeout | undefined;
};

export class Forwarder {
  readonly #lc: LogContext;
  readonly #setTimeout: typeof setTimeout;
  readonly #clearTimeout: typeof clearTimeout;
  readonly #active = new Set<Subscriber>();
  readonly #queued = new Set<Subscriber>();
  readonly #earlyReleaseOptions: EarlyReleaseOptions | undefined;
  readonly #flowControlSlowSubscriberGracePeriodMs: number | undefined;

  readonly #flowControlWaits = getOrCreateCounter(
    'replication',
    'flow_control.waits',
    'Flow-control checkpoints completed, labeled by release mode.',
  );
  readonly #flowControlWaitTime = getOrCreateLatencyHistogram(
    'replication',
    'flow_control.wait_duration',
    'Time replication waits at flow-control checkpoints.',
  );
  #inTransaction = false;

  #currentBroadcast: Broadcast | undefined;
  #progressMonitor: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    opts: ProgressMonitorOptions = {
      flowControlConsensusTimeoutProportion: 2.0,
    },
  ) {
    this.#lc = lc.withContext('component', 'subscriber-progress-monitor');
    this.#setTimeout = opts.setTimeoutFn ?? setTimeout;
    this.#clearTimeout = opts.clearTimeoutFn ?? clearTimeout;

    this.#earlyReleaseOptions =
      opts.flowControlConsensusTimeoutProportion < 0
        ? undefined
        : {
            consensusTimeoutProportion:
              opts.flowControlConsensusTimeoutProportion,
            setTimeoutFn: this.#setTimeout,
            clearTimeoutFn: this.#clearTimeout,
          };
    this.#flowControlSlowSubscriberGracePeriodMs =
      opts.flowControlSlowSubscriberGracePeriodMs;

    getOrCreateGauge(
      'replication',
      'flow_control.active_subscribers',
      'Active change-stream subscribers receiving live changes.',
    ).addCallback(result => result.observe(this.#active.size));

    getOrCreateGauge(
      'replication',
      'flow_control.queued_subscribers',
      'Change-stream subscribers waiting for the current transaction to finish before activation.',
    ).addCallback(result => result.observe(this.#queued.size));

    getOrCreateGauge(
      'replication',
      'flow_control.pending_messages',
      'Downstream change-stream messages not yet acked by subscribers.',
    ).addCallback(result =>
      result.observe(this.#subscriberStats().pendingMessages),
    );

    getOrCreateGauge(
      'replication',
      'flow_control.backlog_messages',
      'Live change-stream messages buffered while subscribers catch up.',
    ).addCallback(result =>
      result.observe(this.#subscriberStats().backlogMessages),
    );

    getOrCreateGauge('replication', 'flow_control.backlog_bytes', {
      description:
        'Live change-stream bytes buffered while subscribers catch up.',
      unit: 'By',
    }).addCallback(result =>
      result.observe(this.#subscriberStats().backlogBytes),
    );

    getOrCreateGauge('replication', 'flow_control.max_backlog_bytes', {
      description:
        'Maximum live change-stream bytes buffered by a single subscriber.',
      unit: 'By',
    }).addCallback(result =>
      result.observe(this.#subscriberStats().maxBacklogBytes),
    );
  }

  startProgressMonitor() {
    clearInterval(this.#progressMonitor);
    this.#progressMonitor = setInterval(
      () => this.checkSubscriberProgress(performance.now()),
      1000,
    );
  }

  /**
   * Runs one progress-monitor tick: samples each active subscriber's change
   * rate, logs slow broadcasts, and disconnects subscribers that have been
   * continuously lagging for longer than the configured grace period.
   *
   * Invoked on an interval by {@link startProgressMonitor()}; exposed (rather
   * than private) so tests can drive a single tick with an explicit `now`.
   */
  checkSubscriberProgress(now: number) {
    let timedOut: Map<Subscriber, SubscriberStats> | undefined;
    let slowestOnTime: SubscriberStats | undefined;

    for (const sub of this.#active) {
      const stats = sub.sampleProcessRate(now).getStats();
      if (stats.missedLastTimeout) {
        (timedOut ??= new Map()).set(sub, stats);
      } else if (
        stats.processRate <= (slowestOnTime?.processRate ?? Infinity)
      ) {
        slowestOnTime = stats;
      }
    }
    this.#currentBroadcast?.logSlowRequests(now);

    if (
      this.#flowControlSlowSubscriberGracePeriodMs &&
      timedOut?.size &&
      slowestOnTime
    ) {
      for (const [sub, stats] of timedOut.entries()) {
        const rate =
          stats.processRate < slowestOnTime.processRate
            ? 'lagging'
            : 'catching-up';
        const laggingDuration = sub.reportChangeRate(now, rate);
        if (laggingDuration >= this.#flowControlSlowSubscriberGracePeriodMs) {
          this.#lc.warn?.(
            `subscriber ${sub.id} has lagged for ${laggingDuration}ms. disconnecting`,
            {stats},
          );
          sub.close(
            StreamTooFarBehind,
            `lagging for over ${this.#flowControlSlowSubscriberGracePeriodMs}ms (${laggingDuration}ms)`,
          );
        }
      }
    }
  }

  stopProgressMonitor() {
    clearInterval(this.#progressMonitor);
  }

  /**
   * `add()` is called in lock step with `Storer.catchup()` so that the
   * two components have an equivalent interpretation of whether a Transaction is
   * currently being streamed.
   */
  add(sub: Subscriber) {
    if (this.#inTransaction) {
      this.#queued.add(sub);
    } else {
      this.#active.add(sub);
    }
  }

  remove(sub: Subscriber) {
    this.#active.delete(sub);
    this.#queued.delete(sub);
    sub.close();
  }

  /**
   * `forward()` is called in lockstep with `Storer.store()` so that the
   * two components have an equivalent interpretation of whether a Transaction is
   * currently being streamed.
   *
   * This version of forward is fire-and-forget, with no flow control. The
   * change-streamer should call and await {@link forwardWithFlowControl()}
   * occasionally to avoid memory blowup.
   */
  forward(entry: WatermarkedChange) {
    Broadcast.withoutTracking(this.#active.values(), entry);
    this.#updateActiveSubscribers(entry[1]);
  }

  sendStatus(status: Status) {
    for (const sub of this.#active.values()) {
      sub.sendStatus(status);
    }
  }

  /**
   * The flow-control-aware equivalent of {@link forward()}, returning a
   * Promise that resolves when replication should continue.
   */
  async forwardWithFlowControl(entry: WatermarkedChange) {
    const start = performance.now();
    const broadcast = new Broadcast(
      this.#lc,
      this.#active.values(),
      entry,
      this.#earlyReleaseOptions,
    );
    this.#updateActiveSubscribers(entry[1]);

    // set for progress tracking
    this.#currentBroadcast = broadcast;

    try {
      await broadcast.done;
      const attributes = {'release.mode': broadcast.releaseMode};
      this.#flowControlWaits.add(1, attributes);
      this.#flowControlWaitTime.recordMs(performance.now() - start, attributes);
    } finally {
      // Technically #currentBroadcast may have changed, so only
      // unset if it if is still the same.
      if (this.#currentBroadcast === broadcast) {
        this.#currentBroadcast = undefined;
      }
    }
  }

  #subscriberStats() {
    let pendingMessages = 0;
    let backlogMessages = 0;
    let backlogBytes = 0;
    let maxBacklogBytes = 0;

    for (const sub of joinIterables(this.#active, this.#queued)) {
      const stats = sub.getStats();
      pendingMessages += stats.pending;
      backlogMessages += stats.backlog;
      backlogBytes += stats.backlogBytes;
      maxBacklogBytes = Math.max(maxBacklogBytes, stats.backlogBytes);
    }

    return {pendingMessages, backlogMessages, backlogBytes, maxBacklogBytes};
  }

  #updateActiveSubscribers(tag: ChangeTag) {
    switch (tag) {
      case 'begin':
        // While in a Transaction, all added subscribers are "queued" so that no
        // messages are forwarded to them. This state corresponds to being queued
        // for catchup in the Storer, which will retrieve historic changes
        // and call catchup() once the current transaction is committed.
        this.#inTransaction = true;
        break;
      case 'commit':
      case 'rollback':
        // Upon commit or rollback, all queued subscribers are transferred to
        // the active set. This means that they can receive messages starting
        // from the next transaction.
        //
        // Note that if catchup is still in progress (in the Storer), these messages
        // will be buffered in the backlog until catchup completes.
        this.#inTransaction = false;
        for (const sub of this.#queued.values()) {
          this.#active.add(sub);
        }
        this.#queued.clear();
        break;
    }
  }

  getAcks(): Set<string> {
    return new Set(
      joinIterables(
        wrapIterable(this.#active).map(s => s.acked),
        wrapIterable(this.#queued).map(s => s.acked),
      ),
    );
  }
}
