import type {Node} from './data.ts';
import {filterPull, type PullStream} from './stream.ts';

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
