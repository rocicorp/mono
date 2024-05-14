import type {Entity} from '../../../../entity.js';
import {genCached, genFilter, genMap} from '../../../util/iterables.js';
import type {Entry, Multiset} from '../../multiset.js';
import type {StringOrNumber, Version} from '../../types.js';
import type {DifferenceStream} from '../difference-stream.js';
import type {Request} from '../message.js';
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
  readonly #seenUpstreamMessages = new Set<number>();
  #lastSeenVersion: Version = -1;

  constructor(input: DifferenceStream<T>, output: DifferenceStream<T>) {
    super(input, output, (version, data) => this.#handleDiff(version, data));
  }

  // TODO(mlaw): if what we're distinct on is being emitted in contiguous chunks we can optimize this.
  // E.g., `or` would be a good example of this.
  #handleDiff(version: number, multiset: Multiset<T>): Multiset<T> {
    if (version > this.#lastSeenVersion) {
      this.#entriesCache.clear();
      this.#lastSeenVersion = version;
    }

    /*
    That distinct is stateful is a problem for laziness.
    Future invocation of the lazy iterable returns different data than the first.
    We need to cache what we've returned and return the same to future callers.

    Lazy reduce should also be a problem for us...
    Probably can see it with `or` and `and` operators on `having` against a reduction.
    */

    const entriesCache = this.#entriesCache;
    return genFilter(
      genCached(
        genMap(multiset, (entry): Entry<T> | undefined => {
          if (entry[1] === 0) {
            return undefined;
          }

          const {id} = entry[0];
          const existingEntry = entriesCache.get(id);

          if (!existingEntry) {
            entriesCache.set(id, entry);
            return [entry[0], Math.sign(entry[1])];
          }

          const newMult = existingEntry[1] + entry[1];
          entriesCache.set(id, [entry[0], newMult]);

          if (existingEntry[1] > 0 && newMult < 0) {
            return [entry[0], -2];
          }

          if (existingEntry[1] === 0 && newMult < 0) {
            return [entry[0], -1];
          }

          if (existingEntry[1] > 0 && newMult === 0) {
            return [entry[0], -1];
          }

          if (existingEntry[1] <= 0 && newMult < 0) {
            return undefined;
          }

          if (existingEntry[1] < 0 && newMult === 0) {
            return [entry[0], 1];
          }

          if (existingEntry[1] === 0 && newMult > 0) {
            return [entry[0], 1];
          }

          if (existingEntry[1] < 0 && newMult > 0) {
            return [entry[0], 2];
          }

          return undefined;
        }),
      ),
      (x): x is Entry<T> => x !== undefined,
    );
  }

  messageUpstream(message: Request): void {
    // TODO(arv): Test this and validate that it is correct.
    if (!this.#seenUpstreamMessages.has(message.id)) {
      this.#seenUpstreamMessages.add(message.id);
      super.messageUpstream(message);
    }
  }
}

export class DistinctAllOperator<T extends object> extends UnaryOperator<T, T> {
  #entriesCache = new Map<StringOrNumber, Entry<T>>();
  #keyFn;

  constructor(
    input: DifferenceStream<T>,
    output: DifferenceStream<T>,
    keyFn: (entry: T) => StringOrNumber,
  ) {
    super(input, output, (_version, data) => this.#handleDiff(data));
    this.#keyFn = keyFn;
  }

  #handleDiff(multiset: Multiset<T>): Multiset<T> {
    return genFilter(
      genMap(multiset, (newEntry): Entry<T> | undefined => {
        const key = this.#keyFn(newEntry[0]);
        const existingEntry = this.#entriesCache.get(key);
        if (existingEntry === undefined) {
          this.#entriesCache.set(key, newEntry);
          return [newEntry[0], Math.sign(newEntry[1])];
        }

        if (existingEntry[1] > 0) {
          const newMult = existingEntry[1] + newEntry[1];
          if (newMult === 0) {
            this.#entriesCache.delete(key);
            return [newEntry[0], -1];
          } else if (newMult < 0) {
            this.#entriesCache.set(key, [newEntry[0], newMult]);
            return [newEntry[0], -1];
          }
          return undefined;
        } else if (existingEntry[1] < 0) {
          const newMult = existingEntry[1] + newEntry[1];
          if (newMult === 0) {
            this.#entriesCache.delete(key);
            return [newEntry[0], -1];
          } else if (newMult > 0) {
            this.#entriesCache.set(key, [newEntry[0], newMult]);
            return [newEntry[0], 1];
          }
          return undefined;
        }

        throw new Error('Null entry found in distinct!');
      }),
      (entry): entry is Entry<T> => entry !== undefined,
    );
  }
}
