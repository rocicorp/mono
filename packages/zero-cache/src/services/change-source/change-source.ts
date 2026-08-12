import type {Sink, Source} from '../../types/streams.ts';
import type {
  BackfillRequest,
  ChangeSourceUpstream,
  ChangeStreamMessage,
} from './protocol/current.ts';

export type ChangeStream = {
  changes: Source<ChangeStreamMessage>;

  /** Resolves if the upstream source fails independently of change consumption. */
  sourceTerminated?: Promise<Error>;

  /**
   * A Sink to push the {@link StatusMessage}s that reflect Commits
   * that have been successfully stored by the {@link Storer}, or
   * downstream {@link StatusMessage}s henceforth.
   */
  acks: Sink<ChangeSourceUpstream>;
}; /** Encapsulates an upstream-specific implementation of a stream of Changes. */

export interface ChangeSource {
  /**
   * Starts a replication lag reporter, returning the commit time of the first
   * report, and the send time of the next expected report, or `null` if lag
   * reporting is not supported / enabled.
   */
  startLagReporter(): Promise<{
    firstCommitTimeMs: number;
    nextSendTimeMs: number;
  } | null> | null;

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
