import {Entity} from '../../../../generate.js';
import {Entry, Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStream} from '../difference-stream.js';
import {Request} from '../message.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * A dataflow operator that has a single input edge and a single output edge.
 * It collapses multiple updates in the same transaction (version) to a single output.
 * The multiplicity of the output entries is -1, 0, or 1 after the tx is done.
 *
 * The id property iu used to identify the entity.
 */
export class DistinctOperator<T extends Entity> extends UnaryOperator<T, T> {
  // The entries for this version. The string is the ID of the entity.
  readonly #entriesCache = new Map<string, Entry<T>>();
  // The emitted data for this version. The string is the ID of the entity. The
  // number is the multiplicity outputted so far in this tx.
  readonly #emitted = new Map<string, number>();
  readonly #seenUpstreamMessages = new Set<number>();
  #lastSeenVersion: Version = -1;

  constructor(input: DifferenceStream<T>, output: DifferenceStream<T>) {
    super(input, output, (version, data) => this.#handleDiff(version, data));
  }

  #handleDiff(version: number, multiset: Multiset<T>): Multiset<T> {
    if (version > this.#lastSeenVersion) {
      this.#entriesCache.clear();
      this.#emitted.clear();
      this.#lastSeenVersion = version;
    }

    const entriesCache = this.#entriesCache;
    const emitted = this.#emitted;

    for (const entry of multiset) {
      const {id} = entry[0];
      const existingEntry = entriesCache.get(id);
      entriesCache.set(
        id,
        existingEntry ? [entry[0], existingEntry[1] + entry[1]] : entry,
      );
    }

    const newMultiset: Entry<T>[] = [];
    for (const [value, multiplicity] of entriesCache.values()) {
      const {id} = value;
      const existingMultiplicity = emitted.get(id) ?? 0;
      const desiredMultiplicity = Math.sign(multiplicity);
      if (existingMultiplicity !== desiredMultiplicity) {
        newMultiset.push([value, desiredMultiplicity - existingMultiplicity]);
        emitted.set(id, desiredMultiplicity);
      }
    }

    return newMultiset;
  }

  messageUpstream(message: Request): void {
    // TODO(arv): Test this and validate that it is correct.
    if (!this.#seenUpstreamMessages.has(message.id)) {
      this.#seenUpstreamMessages.add(message.id);
      super.messageUpstream(message);
    }
  }
}
