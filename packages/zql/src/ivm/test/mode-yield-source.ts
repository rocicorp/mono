/**
 * A source wrapper like `RandomYieldSource`, but able to inject 'yield'
 * sentinels into ONLY the fetch stream, ONLY the push streams, or both.
 *
 * Splitting the two isolates where the take-over-union-fan-in corruption comes
 * from: an interrupted Take maintenance fetch (which reads through UnionFanIn
 * into every OR branch) versus an interrupted push through UnionFanOut ->
 * branches -> UnionFanIn's accumulate/collapse.
 */
import type {Condition, Ordering} from '../../../../zero-protocol/src/ast.ts';
import type {TableSchema} from '../../../../zero-types/src/schema.ts';
import type {DebugDelegate} from '../../builder/debug-delegate.ts';
import type {Node} from '../data.ts';
import type {FetchRequest} from '../operator.ts';
import type {Source, SourceChange, SourceInput} from '../source.ts';
import type {Stream} from '../stream.ts';

export type YieldMode = 'fetch' | 'push' | 'both';

export class ModeYieldSource implements Source {
  readonly #source: Source;
  readonly #rng: () => number;
  readonly #p: number;
  readonly #mode: YieldMode;

  constructor(
    source: Source,
    rng: () => number,
    mode: YieldMode,
    yieldProbability = 0.3,
  ) {
    this.#source = source;
    this.#rng = rng;
    this.#mode = mode;
    this.#p = yieldProbability;
  }

  get tableSchema(): TableSchema {
    return this.#source.tableSchema;
  }

  get #yieldsInFetch() {
    return this.#mode === 'fetch' || this.#mode === 'both';
  }

  get #yieldsInPush() {
    return this.#mode === 'push' || this.#mode === 'both';
  }

  connect(
    sort: Ordering | undefined,
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
    if (!this.#yieldsInFetch) {
      return sourceInput;
    }
    const rng = this.#rng;
    const p = this.#p;
    const originalFetch = sourceInput.fetch.bind(sourceInput);
    return {
      ...sourceInput,
      *fetch(req: FetchRequest): Stream<Node | 'yield'> {
        for (const item of originalFetch(req)) {
          if (rng() < p) {
            yield 'yield';
          }
          yield item;
        }
        if (rng() < p) {
          yield 'yield';
        }
      },
    };
  }

  *push(change: SourceChange): Stream<'yield'> {
    for (const item of this.#source.push(change)) {
      if (this.#yieldsInPush && this.#rng() < this.#p) {
        yield 'yield';
      }
      if (item === 'yield') {
        yield item;
      }
    }
    if (this.#yieldsInPush && this.#rng() < this.#p) {
      yield 'yield';
    }
  }

  *genPush(change: SourceChange): Stream<'yield' | undefined> {
    for (const item of this.#source.genPush(change)) {
      if (this.#yieldsInPush && this.#rng() < this.#p) {
        yield 'yield';
      }
      yield item;
    }
    if (this.#yieldsInPush && this.#rng() < this.#p) {
      yield 'yield';
    }
  }
}

export function wrapSourcesWithModeYield(
  sources: Record<string, Source>,
  rng: () => number,
  mode: YieldMode,
  yieldProbability = 0.3,
): Record<string, Source> {
  return Object.fromEntries(
    Object.entries(sources).map(([k, s]) => [
      k,
      new ModeYieldSource(s, rng, mode, yieldProbability),
    ]),
  );
}
