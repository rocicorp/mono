import {must} from 'shared/src/must.js';
import type {Entity} from '../../entity.js';
import {
  buildPipeline,
  getValueFromEntity,
  selectorsToQualifiedColumns,
} from '../ast-to-ivm/pipeline-builder.js';
import type {AST} from '../ast/ast.js';
import type {Context} from '../context/context.js';
import {compareEntityFields} from '../ivm/compare.js';
import type {DifferenceStream} from '../ivm/graph/difference-stream.js';
import {MutableTreeView} from '../ivm/view/tree-view.js';
import type {View} from '../ivm/view/view.js';
import type {MakeHumanReadable} from './entity-query.js';

export interface IStatement<TReturn> {
  subscribe(cb: (value: MakeHumanReadable<TReturn>) => void): () => void;
  exec(): Promise<MakeHumanReadable<TReturn>>;
  view(): View<TReturn>;
  destroy(): void;
}

export class Statement<Return> implements IStatement<Return> {
  readonly #pipeline;
  readonly #ast;
  readonly #context;
  #materialization: View<Return extends [] ? Return[number] : Return> | null =
    null;

  constructor(context: Context, ast: AST) {
    this.#ast = ast;
    this.#pipeline = buildPipeline(
      <T extends Entity>(sourceName: string) =>
        context.getSource(sourceName).stream as unknown as DifferenceStream<T>,
      ast,
    );
    this.#context = context;
  }

  view(): View<Return> {
    // TODO: invariants to throw if the statement is not completely bound before materialization.
    if (this.#materialization === null) {
      this.#materialization = new MutableTreeView<
        Return extends [] ? Return[number] : never
      >(
        this.#context.materialite,
        this.#pipeline as unknown as DifferenceStream<
          Return extends [] ? Return[number] : never
        >,
        makeComparator<readonly string[], Record<string, unknown>>(
          must(this.#ast.orderBy)[0],
          must(this.#ast.orderBy)[1],
        ),
        this.#ast.orderBy,
        this.#ast.limit,
      ) as unknown as View<Return extends [] ? Return[number] : Return>;
    }

    this.#materialization.pullHistoricalData();

    return this.#materialization as View<Return>;
  }

  subscribe(cb: (value: MakeHumanReadable<Return>) => void) {
    if (this.#materialization === null) {
      this.view();
    }

    return must(this.#materialization).on(cb);
  }

  // Note: should we provide a version that takes a callback?
  // So it can resolve in the same micro task?
  // since, in the common case, the data will always be available.
  exec() {
    if (this.#materialization === null) {
      this.view();
    }

    if (this.#materialization?.hydrated) {
      return Promise.resolve(this.#materialization.value) as Promise<
        MakeHumanReadable<Return>
      >;
    }

    return new Promise<MakeHumanReadable<Return>>(resolve => {
      const cleanup = must(this.#materialization).on(value => {
        resolve(value as MakeHumanReadable<Return>);
        cleanup();
      });
    }) as Promise<MakeHumanReadable<Return>>;
  }

  // For savvy users that want to subscribe directly to diffs.
  // onDifference() {}

  destroy() {
    this.#pipeline.destroy();
  }
}

export function makeComparator<
  Keys extends ReadonlyArray<keyof T>,
  T extends object,
>(sortKeys: Keys, direction: 'asc' | 'desc'): (l: T, r: T) => number {
  const qualifiedColumns = selectorsToQualifiedColumns(
    sortKeys as unknown as string[],
  );
  const comparator = (l: T, r: T) => {
    let comp = 0;
    for (const qualifiedColumn of qualifiedColumns) {
      comp = compareEntityFields(
        getValueFromEntity(l as Record<string, unknown>, qualifiedColumn),
        getValueFromEntity(r as Record<string, unknown>, qualifiedColumn),
      );
      if (comp !== 0) {
        return comp;
      }
    }

    return comp;
  };

  return direction === 'asc' ? comparator : (l, r) => comparator(r, l);
}
