import type {Node} from './data.ts';
import {filterPull, forEachPull, type PullStream} from './stream.ts';

/**
 * Drops the 'yield' markers from a stream.
 *
 * The cast is the one place that knows dropping every 'yield' leaves only
 * Nodes; `filterPull` cannot narrow its own element type.
 */
export function skipYields(
  stream: PullStream<Node | 'yield'>,
): PullStream<Node> {
  return filterPull(stream, v => v !== 'yield') as PullStream<Node>;
}

/**
 * `forEachPull` over the nodes of a stream, dropping the 'yield' markers and
 * closing the stream when the scan ends, breaks or throws.
 *
 * Named for what it discards. A consumer that can suspend must propagate the
 * markers rather than drop them, so reaching for this in a suspendable context
 * is a bug -- it should be obvious at the call site which one you picked.
 */
export function forEachSkippingYields(
  stream: PullStream<Node | 'yield'>,
  fn: (node: Node) => unknown,
): void {
  forEachPull(skipYields(stream), fn);
}
