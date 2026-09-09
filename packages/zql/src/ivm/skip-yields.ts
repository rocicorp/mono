import type {Node} from './data.ts';
import {PullStreamBase, type PullStream} from './stream.ts';

/**
 * Drops the 'yield' markers from a stream.
 *
 * A pull stream, so no iterator object and no per-value result object: the
 * loop below just keeps pulling until it sees something that is not 'yield'.
 */
class SkipYieldsStream extends PullStreamBase<Node> {
  readonly #stream: PullStream<Node | 'yield'>;

  constructor(stream: PullStream<Node | 'yield'>) {
    super();
    this.#stream = stream;
  }

  next(): Node | undefined {
    for (;;) {
      const v = this.#stream.next();
      if (v !== 'yield') {
        return v;
      }
    }
  }

  close(): void {
    this.#stream.close();
  }
}

export function skipYields(
  stream: PullStream<Node | 'yield'>,
): PullStream<Node> {
  return new SkipYieldsStream(stream);
}
