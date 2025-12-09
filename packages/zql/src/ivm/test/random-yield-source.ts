import type {Condition, Ordering} from '../../../../zero-protocol/src/ast.ts';
import type {DebugDelegate} from '../../builder/debug-delegate.ts';
import type {FetchRequest} from '../operator.ts';
import type {Source, SourceChange, SourceInput} from '../source.ts';
import type {Stream} from '../stream.ts';
import type {Node} from '../data.ts';

/**
 * A source wrapper that randomly injects 'yield' values into fetch and push
 * streams for testing cooperative scheduling.
 */
export class RandomYieldSource implements Source {
  readonly #source: Source;
  readonly #rng: () => number;
  readonly #yieldProbability: number;

  /**
   * @param source The underlying source to wrap
   * @param rng Random number generator returning values in [0, 1)
   * @param yieldProbability Probability of yielding at each yield point (0 to 1)
   */
  constructor(
    source: Source,
    rng: () => number,
    yieldProbability: number = 0.3,
  ) {
    this.#source = source;
    this.#rng = rng;
    this.#yieldProbability = yieldProbability;
  }

  get tableSchema() {
    return this.#source.tableSchema;
  }

  connect(
    sort: Ordering,
    filters?: Condition,
    splitEditKeys?: Set<string>,
    debug?: DebugDelegate,
  ): SourceInput {
    const sourceInput = this.#source.connect(
      sort,
      filters,
      splitEditKeys,
      debug,
    );
    const rng = this.#rng;
    const yieldProbability = this.#yieldProbability;

    const originalFetch = sourceInput.fetch.bind(sourceInput);

    const wrappedInput: SourceInput = {
      ...sourceInput,
      *fetch(req: FetchRequest): Stream<Node | 'yield'> {
        for (const item of originalFetch(req)) {
          // Randomly yield before each item
          if (rng() < yieldProbability) {
            yield 'yield';
          }
          yield item;
        }
        // Randomly yield at the end
        if (rng() < yieldProbability) {
          yield 'yield';
        }
      },
    };

    return wrappedInput;
  }

  *push(change: SourceChange): Stream<'yield'> {
    for (const item of this.#source.push(change)) {
      // Randomly yield before each yield from underlying source
      if (this.#rng() < this.#yieldProbability) {
        yield 'yield';
      }
      if (item === 'yield') {
        yield item;
      }
    }
    // Randomly yield at the end
    if (this.#rng() < this.#yieldProbability) {
      yield 'yield';
    }
  }

  *genPush(change: SourceChange): Stream<'yield' | undefined> {
    for (const item of this.#source.genPush(change)) {
      // Randomly yield before each item
      if (this.#rng() < this.#yieldProbability) {
        yield 'yield';
      }
      yield item;
    }
    // Randomly yield at the end
    if (this.#rng() < this.#yieldProbability) {
      yield 'yield';
    }
  }
}

/**
 * Wraps all sources in a record with RandomYieldSource.
 */
export function wrapSourcesWithRandomYield(
  sources: Record<string, Source>,
  rng: () => number,
  yieldProbability: number = 0.3,
): Record<string, Source> {
  return Object.fromEntries(
    Object.entries(sources).map(([key, source]) => [
      key,
      new RandomYieldSource(source, rng, yieldProbability),
    ]),
  );
}
