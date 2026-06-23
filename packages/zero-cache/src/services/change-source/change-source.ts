import type {Sink, Source} from '../../types/streams.ts';
import type {
  BackfillRequest,
  ChangeSourceUpstream,
  ChangeStreamMessage,
} from './protocol/current.ts';

export type ChangeStream = {
  changes: Source<ChangeStreamMessage>;

  /**
   * A Sink to push the {@link StatusMessage}s that reflect Commits
   * that have been successfully stored by the {@link Storer}, or
   * downstream {@link StatusMessage}s henceforth.
   */
  acks: Sink<ChangeSourceUpstream>;

  /**
   * Caps upstream acknowledgements at the given (backed-up) watermark, so the
   * source never advances its position past what has been durably backed up
   * (RMv2 backup-driven ACK). Calling it enables backup-gating; it is then
   * called again whenever the backup watermark advances. Optional: only change
   * sources with an externally-durable position (e.g. a Postgres replication
   * slot) implement it.
   */
  setBackupWatermark?: ((watermark: string) => void) | undefined;
}; /** Encapsulates an upstream-specific implementation of a stream of Changes. */

export interface ChangeSource {
  /**
   * Starts a replication lag reporter, returning the send time of the next
   * expected report, or `null` if lag reporting is not supported / enabled.
   */
  startLagReporter(): Promise<{nextSendTimeMs: number} | null> | null;

  /**
   * Starts a stream of changes starting after the specific watermark,
   * with a corresponding sink for upstream acknowledgements.
   */
  startStream(
    afterWatermark: string,
    backfillRequests?: BackfillRequest[],
  ): Promise<ChangeStream>;

  /**
   * Releases connections and resources held by this change source.
   */
  stop(): Promise<void>;
}
