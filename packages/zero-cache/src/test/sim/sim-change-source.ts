import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import type {
  ChangeSource,
  ChangeStream,
} from '../../services/change-source/change-source.ts';
import {startMultiplexedChangeStream} from '../../services/change-source/common/multiplexed-change-stream.ts';
import type {ChangeStreamData} from '../../services/change-source/protocol/current/downstream.ts';
import type {BackfillRequest} from '../../services/change-source/protocol/current/upstream.ts';
import {inFencedIncarnation} from './incarnation.ts';
import {TERMINATED_BY_TAKEOVER, type SimPG} from './sim-pg.ts';

export type SimChangeSourceOptions = {
  /** Rows per `backfill` message, which stands in for a COPY chunk. */
  readonly batchRows: number;
  /** `BackfillOptions.commitThresholdBytes`. */
  readonly commitThresholdBytes: number;
  /** `BackfillOptions.resume`: whether runs are ordered, and so resumable. */
  readonly resume: boolean;
  /** Called as each stream starts, before it takes the slot. */
  readonly onStreamStart?: (() => void) | undefined;
};

/**
 * The PG change source's stream over SimPG: the real multiplexer,
 * `BackfillManager`, reservation loop and `Acker` of
 * `startMultiplexedChangeStream`, with SimPG's slot as the upstream and its
 * snapshots as the backfill streamer. Only turning pgoutput into changes is
 * left out, since SimPG streams changes.
 */
export class SimChangeSource implements ChangeSource {
  readonly #lc: LogContext;
  readonly #pg: SimPG;
  readonly #opts: SimChangeSourceOptions;
  #takenOver = false;

  constructor(lc: LogContext, pg: SimPG, opts: SimChangeSourceOptions) {
    this.#lc = lc.withContext('component', 'change-source');
    this.#pg = pg;
    this.#opts = opts;
  }

  /** Whether a stream of this source ended because another took the slot. */
  get takenOver(): boolean {
    return this.#takenOver;
  }

  startLagReporter(): null {
    return null;
  }

  startStream(
    clientWatermark: string,
    backfillRequests: BackfillRequest[] = [],
  ): Promise<ChangeStream> {
    if (inFencedIncarnation()) {
      return new Promise(() => {});
    }
    this.#opts.onStreamStart?.();
    const pg = this.#pg;
    const stream = pg.openStream(clientWatermark);
    const {changes} = stream;
    const {batchRows, commitThresholdBytes, resume} = this.#opts;
    return Promise.resolve(
      startMultiplexedChangeStream(
        this.#lc,
        clientWatermark,
        backfillRequests,
        {
          messages: {
            [Symbol.asyncIterator]: () => changes[Symbol.asyncIterator](),
            cancel: (err?: Error) => changes.cancel(err),
            signal: changes.signal,
            // Messages still on the wire are queued too, as the PG source's
            // are in postgres.js's buffer.
            get queued() {
              return stream.queued;
            },
          },
          acks: {push: lsn => pg.ackLSN(stream, lsn)},
        },
        {
          // Keepalives are the only messages SimPG streams outside a
          // transaction.
          isTransactional: (msg, pushStatus): msg is ChangeStreamData => {
            if (msg[0] === 'status') {
              pushStatus(msg);
              return false;
            }
            return true;
          },
          makeChanges: msg => Promise.resolve([msg]),
          onError: e => {
            // What the PG source's `translateError` makes of the
            // `pg_terminate_backend` of a takeover: a `ShutdownSignal`, which
            // stops the change-streamer instead of retrying it.
            if (e instanceof Error && e.message === TERMINATED_BY_TAKEOVER) {
              this.#takenOver = true;
              return Promise.resolve(
                new AbortError(TERMINATED_BY_TAKEOVER, {cause: e}),
              );
            }
            return Promise.resolve(
              e instanceof Error ? e : new Error(String(e)),
            );
          },
        },
        {
          streamer: req =>
            pg.streamBackfill(req, {runID: pg.nextRunID(), batchRows, resume}),
          rowsExist: (req, from, to) => pg.rowsExist(req, from, to),
          commitThresholdBytes,
        },
      ),
    );
  }

  stop(): Promise<void> {
    return Promise.resolve();
  }
}
