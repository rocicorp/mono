import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import {makeComparator} from '../../../query/statement.js';
import {gen} from '../../../util/iterables.js';
import type {Entry, Multiset} from '../../multiset.js';
import {sourcesAreIdentical} from '../../source/util.js';
import type {DifferenceStream, Listener} from '../difference-stream.js';
import type {PullMsg, Reply} from '../message.js';
import type {Operator} from './operator.js';

let id = 0;

/**
 * A dataflow operator (node) that has many incoming edges and
 * one outgoing edge (write handle). It just sends all the input messages from
 * all the incoming operator to the output operators.
 */
export class ConcatOperator<T extends object> implements Operator {
  readonly #listener: Listener<T>;
  readonly #inputs: DifferenceStream<T>[];
  readonly #output: DifferenceStream<T>;
  readonly #id = id++;

  readonly #replyBuffer: [multiset: Multiset<T>, reply: Reply][] = [];
  #replyVersion: number = -1;

  constructor(inputs: DifferenceStream<T>[], output: DifferenceStream<T>) {
    this.#inputs = inputs;
    this.#output = output;
    this.#listener = {
      newDifference: (version, data, reply) => {
        if (reply !== undefined && this.#inputs.length > 1) {
          this.#replyBuffer.push([data, reply]);
          this.#replyVersion = version;
          if (this.#replyBuffer.length === this.#inputs.length) {
            this.#flushReplyBuffer();
          }
        } else {
          output.newDifference(version, data, reply);
        }
      },
      commit: version => {
        this.commit(version);
      },
    };
    for (const input of inputs) {
      input.addDownstream(this.#listener);
    }
  }

  get id() {
    return this.#id;
  }

  #flushReplyBuffer() {
    const first = this.#replyBuffer[0];
    const allIdentical = this.#replyBuffer.every(
      b =>
        first[1].order !== undefined &&
        b[1].order !== undefined &&
        sourcesAreIdentical(
          first[1].sourceName,
          first[1].order,
          b[1].sourceName,
          b[1].order,
        ),
    );

    if (!allIdentical) {
      for (const buffered of this.#replyBuffer) {
        this.#output.newDifference(
          this.#replyVersion,
          buffered[0],
          buffered[1],
        );
      }
    } else {
      this.#output.newDifference(
        this.#replyVersion,
        gen(() => genInOrder(this.#replyBuffer)),
        first[1],
      );
    }

    this.#replyBuffer.length = 0;
    this.#replyVersion = -1;
  }

  commit(version: number): void {
    assert(
      this.#replyBuffer.length === 0,
      'Receive commit before flushing responses to replies!',
    );
    this.#output.commit(version);
  }

  messageUpstream(message: PullMsg): void {
    for (const input of this.#inputs) {
      input.messageUpstream(message, this.#listener);
    }
  }

  destroy() {
    for (const input of this.#inputs) {
      input.removeDownstream(this.#listener);
    }
  }
}

export function* genInOrder<T extends object>(
  buffer: [multiset: Multiset<T>, reply: Reply][],
) {
  const first = buffer[0];
  const order = must(first[1].order);
  const comparator = makeComparator(order[0], order[1]);

  const iterators = buffer.map(r => r[0][Symbol.iterator]());
  try {
    const current = iterators.map(it => it.next());
    while (current.some(c => !c.done)) {
      const min = current.reduce(
        (
          acc: [Entry<T>, number] | undefined,
          c,
          i,
        ): [Entry<T>, number] | undefined => {
          if (c.done) {
            return acc;
          }
          if (acc === undefined || comparator(c.value[0], acc[0]) < 0) {
            return [c.value, i];
          }
          return acc;
        },
        undefined,
      );

      assert(min !== undefined, 'min is undefined');
      yield min[0];
      current[min[1]] = iterators[min[1]].next();
    }
  } finally {
    for (const it of iterators) {
      it.return?.();
    }
  }
}
