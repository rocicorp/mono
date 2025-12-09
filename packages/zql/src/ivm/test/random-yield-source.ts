import type {Condition, Ordering} from '../../../../zero-protocol/src/ast.ts';
import type {DebugDelegate} from '../../builder/debug-delegate.ts';
import type {FetchRequest} from '../operator.ts';
import type {Source, SourceChange, SourceInput} from '../source.ts';
import type {Stream} from '../stream.ts';
import type {Node} from '../data.ts';

/**
 * A source wrapper that randomly injects 'yield' values into fetch and push
 * streams for testing cooperative scheduling. Can also check for timeout
 * conditions via an optional shouldYield callback that can throw to abort.
 */
export class RandomYieldSource implements Source {
  readonly #source: Source;
  readonly #rng: () => number;
  readonly #yieldProbability: number;
  readonly #checkAbort: (() => void) | undefined;

  /**
   * @param source The underlying source to wrap
   * @param rng Random number generator returning values in [0, 1)
   * @param yieldProbability Probability of yielding at each yield point (0 to 1)
   * @param checkAbort Optional callback called at each yield point that can throw to abort
   */
  constructor(
    source: Source,
    rng: () => number,
    yieldProbability: number = 0.3,
    checkAbort?: () => void,
  ) {
    this.#source = source;
    this.#rng = rng;
    this.#yieldProbability = yieldProbability;
    this.#checkAbort = checkAbort;
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
    const checkAbort = this.#checkAbort;

    const originalFetch = sourceInput.fetch.bind(sourceInput);

    const wrappedInput: SourceInput = {
      ...sourceInput,
      *fetch(req: FetchRequest): Stream<Node | 'yield'> {
        for (const item of originalFetch(req)) {
          // Check for abort (can throw)
          checkAbort?.();
          // Randomly yield before each item
          if (rng() < yieldProbability) {
            yield 'yield';
          }
          yield item;
        }
        // Check for abort at the end
        checkAbort?.();
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
      // Check for abort (can throw)
      this.#checkAbort?.();
      // Randomly yield before each yield from underlying source
      if (this.#rng() < this.#yieldProbability) {
        yield 'yield';
      }
      if (item === 'yield') {
        yield item;
      }
    }
    // Check for abort at the end
    this.#checkAbort?.();
    // Randomly yield at the end
    if (this.#rng() < this.#yieldProbability) {
      yield 'yield';
    }
  }

  *genPush(change: SourceChange): Stream<'yield' | undefined> {
    for (const item of this.#source.genPush(change)) {
      // Check for abort (can throw)
      this.#checkAbort?.();
      // Randomly yield before each item
      if (this.#rng() < this.#yieldProbability) {
        yield 'yield';
      }
      yield item;
    }
    // Check for abort at the end
    this.#checkAbort?.();
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
  checkAbort?: () => void,
): Record<string, Source> {
  return Object.fromEntries(
    Object.entries(sources).map(([key, source]) => [
      key,
      new RandomYieldSource(source, rng, yieldProbability, checkAbort),
    ]),
  );
}

/**
 * Creates a source wrapper function that wraps sources with RandomYieldSource.
 * Useful for passing to delegates that need to wrap sources on-demand.
 */
export function createRandomYieldWrapper(
  rng: () => number,
  yieldProbability: number = 0.3,
  checkAbort?: () => void,
): (source: Source) => Source {
  return source => new RandomYieldSource(source, rng, yieldProbability, checkAbort);
}
