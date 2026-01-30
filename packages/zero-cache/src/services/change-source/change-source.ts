import type {Sink, Source} from '../../types/streams.ts';
import type {
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
}; /** Encapsulates an upstream-specific implementation of a stream of Changes. */

export interface ChangeSource {
  /**
   * Starts a stream of changes starting after the specific watermark,
   * with a corresponding sink for upstream acknowledgements.
   */
  startStream(afterWatermark: string): Promise<ChangeStream>;
}
