import type {LogContext} from '@rocicorp/logger';
import type {LexiVersion} from '../../../types/lexi-version.ts';
import {majorVersionFromString} from '../../../types/state-version.ts';
import type {Sink, Source} from '../../../types/streams.ts';
import type {ChangeStream} from '../change-source.ts';
import type {
  BackfillRequest,
  ChangeStreamMessage,
  DownstreamStatusMessage,
} from '../protocol/current.ts';
import {
  BackfillManager,
  type BackfillStreamer,
  type RowsExist,
} from './backfill-manager.ts';
import {
  ChangeStreamMultiplexer,
  type Listener,
} from './change-stream-multiplexer.ts';

// Parameterize this if necessary. In practice starvation may never happen.
const MAX_LOW_PRIORITY_DELAY_MS = 1000;

type ReservationState = {
  lastWatermark?: string;
};

/** An upstream replication stream, as a multiplexed change stream reads it. */
export type UpstreamStream<T> = {
  /**
   * The upstream's messages. `queued` is how many more are immediately
   * available, which decides whether a commit releases the change stream.
   */
  readonly messages: Source<T> & {readonly queued: number};

  /** Acknowledges upstream positions as the change-streamer acks them. */
  readonly acks: Sink<bigint>;
};

/** What a multiplexed change stream needs to know about upstream messages. */
export type UpstreamMessageHandler<T, Transactional extends T> = {
  /**
   * Whether `msg` belongs to a transaction. One that doesn't, such as a
   * keepalive or a lag report, must not reserve the change stream: it pushes
   * whatever status it carries instead.
   */
  isTransactional(
    msg: T,
    pushStatus: (status: DownstreamStatusMessage) => void,
  ): msg is Transactional;

  /** The changes a transactional message makes, in order. */
  makeChanges(msg: Transactional): Promise<ChangeStreamMessage[]>;

  /** The error to fail the change stream with when reading upstream fails. */
  onError(e: unknown): Promise<Error>;
};

/** How backfills requested of a multiplexed change stream are streamed. */
export type BackfillSource = {
  readonly streamer: BackfillStreamer;
  readonly rowsExist: RowsExist;
  readonly commitThresholdBytes?: number | undefined;
};

/**
 * Starts the change stream of an upstream replication stream, with backfills
 * streamed beside it.
 *
 * This is the part of a change source that does not depend on its upstream: the
 * {@link ChangeStreamMultiplexer} on which the main stream and the
 * {@link BackfillManager}'s backfill streams take turns, the main stream's
 * reservation loop and the policy by which it releases the change stream, the
 * {@link Acker}, and the routing of what the change-streamer sends back.
 */
export function startMultiplexedChangeStream<T, Transactional extends T>(
  lc: LogContext,
  clientWatermark: string,
  backfillRequests: BackfillRequest[],
  {messages, acks}: UpstreamStream<T>,
  handler: UpstreamMessageHandler<T, Transactional>,
  backfill: BackfillSource,
): ChangeStream {
  const acker = new Acker(acks, clientWatermark);

  // The ChangeStreamMultiplexer facilitates cooperative streaming from
  // the main replication stream and backfill streams initiated by the
  // BackfillManager.
  const changes = new ChangeStreamMultiplexer(lc, clientWatermark);
  const backfillManager = new BackfillManager(
    lc,
    changes,
    backfill.streamer,
    backfill.rowsExist,
    undefined,
    undefined,
    undefined,
    backfill.commitThresholdBytes,
  );
  changes
    .addProducers(messages, backfillManager)
    .addListeners(backfillManager, acker);
  backfillManager.run(clientWatermark, backfillRequests);

  const pushStatus = (status: DownstreamStatusMessage) =>
    changes.pushStatus(status);

  void (async () => {
    try {
      let reservation: ReservationState | null = null;
      let inTransaction = false;

      for await (const msg of messages) {
        if (!handler.isTransactional(msg, pushStatus)) {
          // If we're not in a transaction but the last reservation was kept
          // because of pending keepalives or lag reports in the queue,
          // release the reservation.
          if (!inTransaction && reservation?.lastWatermark) {
            changes.release(reservation.lastWatermark);
            reservation = null;
          }
          continue;
        }

        if (!reservation) {
          const res = changes.reserve('replication');
          const lastWatermark = typeof res === 'string' ? res : await res;
          reservation = {lastWatermark};
        }

        let lastChange: ChangeStreamMessage | undefined;
        for (const change of await handler.makeChanges(msg)) {
          await changes.push(change); // Allow the change-streamer to push back.
          lastChange = change;
        }

        switch (lastChange?.[0]) {
          case 'begin':
            inTransaction = true;
            break;
          case 'commit':
            inTransaction = false;
            reservation.lastWatermark = lastChange[2].watermark;
            if (
              messages.queued === 0 ||
              changes.waiterDelay() > MAX_LOW_PRIORITY_DELAY_MS
            ) {
              // After each transaction, release the reservation:
              // - if there are no pending upstream messages
              // - or if a low priority request has been waiting for longer
              //   than MAX_LOW_PRIORITY_DELAY_MS. This is to prevent
              //   (backfill) starvation on very active upstreams.
              changes.release(reservation.lastWatermark);
              reservation = null;
            }
            break;
        }
      }
    } catch (e) {
      // Note: no need to worry about reservations here since downstream
      //       is being completely canceled.
      changes.fail(await handler.onError(e));
    }
  })();

  return {
    changes: changes.asSource(),
    acks: {
      push: msg => {
        if (msg[0] === 'status') {
          acker.ack(msg[2].watermark);
        } else {
          // A subscriber's declared backfill progress, forwarded by the
          // change-streamer because it could not resolve it from its own
          // change log. Handled asynchronously (it may query upstream); a
          // failure costs a backfill that restarts rather than resumes.
          void backfillManager
            .onBackfillRequest(msg)
            .catch(e => lc.warn?.(`error handling a backfill request`, e));
        }
      },
    },
  };
}

// Exported for testing.
export class Acker implements Listener {
  #acks: Sink<bigint>;
  #waitingForDownstreamAck: string | null;

  /**
   * @param resumeWatermark the watermark the stream resumes after. What came
   *     before it was received on an earlier connection, and only the
   *     change-streamer knows whether it has persisted it, so keepalives are
   *     not acked until the change-streamer has acked it. A change-streamer
   *     whose SQLite change log is ahead of its backup would otherwise have
   *     the slot moved past transactions that only that log holds.
   */
  constructor(acks: Sink<bigint>, resumeWatermark: string | null) {
    this.#acks = acks;
    this.#waitingForDownstreamAck = resumeWatermark;
  }

  onChange(change: ChangeStreamMessage): void {
    switch (change[0]) {
      case 'status':
        const {watermark} = change[2];
        if (change[1].ack) {
          this.#expectDownstreamAck(watermark);
        } else {
          // Keepalives with shouldRespond = false are sent to Listeners,
          // but for efficiency they are not sent downstream to the
          // change-streamer. Ack them here if the change-streamer is caught
          // up. This updates the replication slot's `confirmed_flush_lsn`
          // more quickly (rather than waiting for the periodic shouldRespond),
          // which is useful for monitoring replication slot lag.
          this.#ackIfDownstreamIsCaughtUp(watermark);
        }
        break;
      case 'begin':
        // Mark the commit watermark as being expected so that any intermediate
        // shouldRespond=false watermarks, which will be at the
        // commitWatermark, are *not* acked, as the ack must come from
        // change-streamer after it commits the transaction.
        if (!change[1].skipAck) {
          this.#expectDownstreamAck(change[2].commitWatermark);
        }
        break;
    }
  }

  #expectDownstreamAck(watermark: string) {
    this.#waitingForDownstreamAck = watermark;
  }

  ack(watermark: LexiVersion) {
    if (
      this.#waitingForDownstreamAck &&
      this.#waitingForDownstreamAck <= watermark
    ) {
      this.#waitingForDownstreamAck = null;
    }
    this.#sendAck(watermark);
  }

  #ackIfDownstreamIsCaughtUp(watermark: string) {
    if (this.#waitingForDownstreamAck === null) {
      this.#sendAck(watermark);
    }
  }

  #sendAck(watermark: LexiVersion) {
    const lsn = majorVersionFromString(watermark);
    this.#acks.push(lsn);
  }
}
