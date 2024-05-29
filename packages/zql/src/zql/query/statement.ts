import {resolver} from '@rocicorp/resolver';
import {assert} from 'shared/src/asserts.js';
import {
  buildPipeline,
  pullUsedSources,
} from '../ast-to-ivm/pipeline-builder.js';
import type {AST, Ordering, Selector} from '../ast/ast.js';
import type {Context} from '../context/context.js';
import {makeComparator} from '../ivm/compare.js';
import type {DifferenceStream} from '../ivm/graph/difference-stream.js';
import {TreeView} from '../ivm/view/tree-view.js';
import type {View} from '../ivm/view/view.js';
import type {MakeHumanReadable} from './entity-query.js';

export type ResultType = 'complete' | 'partial' | 'none';

export interface IStatement<TReturn> {
  subscribe(cb: (value: MakeHumanReadable<TReturn>) => void): () => void;
  exec(): PromiseLike<MakeHumanReadable<TReturn>>;
  destroy(): void;
}

export class Statement<Return> implements IStatement<Return> {
  readonly #ast;
  readonly #context;
  #materialization?:
    | PromiseLike<View<Return extends [] ? Return[number] : Return>>
    | undefined = undefined;

  constructor(context: Context, ast: AST) {
    this.#ast = ast;
    this.#context = context;
  }

  #getMaterialization(): PromiseLike<View<Return>> {
    if (this.#materialization === undefined) {
      this.#materialization = createMaterialization(this.#ast, this.#context);
    }
    return this.#materialization;
  }

  preload(): {
    cleanup: () => void;
    preloaded: Promise<boolean>;
  } {
    const {resolve, promise: preloaded} = resolver<boolean>();
    const subscriptionRemoved = this.#context.subscriptionAdded(
      this.#ast,
      got => {
        if (got) {
          resolve(true);
        }
      },
    );
    const cleanup = () => {
      subscriptionRemoved();
      resolve(false);
    };
    return {cleanup, preloaded};
  }

  subscribe(
    callback: (
      value: MakeHumanReadable<Return>,
      resultType: ResultType,
    ) => void,
    initialData = true,
  ) {
    let resultType: ResultType = 'none';
    const materialization = this.#getMaterialization();
    const subscriptionRemoved = this.#context.subscriptionAdded(
      this.#ast,
      got => {
        if (got) {
          resultType = 'complete';
        }
        // When we get the gotQueries signal, we need to call the callback since
        // the result might be empty and the view won't trigger a change.
        this.exec()
          .then(v => {
            callback(v, resultType);
          })
          .catch(e => console.error(e));
      },
    );
    const cleanupPromise = materialization.then(view =>
      view.on(v => {
        if (resultType === 'none') {
          resultType = 'partial';
        }
        callback(v, resultType);
      }, initialData),
    );
    const cleanup = () => {
      subscriptionRemoved();
      void cleanupPromise.then(p => p());
    };

    return cleanup;
  }

  // Note: should we provide a version that takes a callback?
  // So it can resolve in the same micro task?
  // since, in the common case, the data will always be available.
  async exec() {
    const materialization = await this.#getMaterialization();

    if (materialization.hydrated) {
      return Promise.resolve(materialization.value) as Promise<
        MakeHumanReadable<Return>
      >;
    }

    return new Promise<MakeHumanReadable<Return>>(resolve => {
      const cleanup = materialization.on(value => {
        resolve(value as MakeHumanReadable<Return>);
        cleanup();
      }, true);
    }) as Promise<MakeHumanReadable<Return>>;
  }

  // For savvy users that want to subscribe directly to diffs.
  // onDifference();

  destroy() {
    void this.#materialization?.then(v => v.destroy());
  }
}

async function createMaterialization<Return>(ast: AST, context: Context) {
  const {orderBy, limit} = ast;
  assert(orderBy);

  const usedSources = pullUsedSources(ast, new Set<string>());
  const promises: PromiseLike<void>[] = [];
  for (const source of usedSources) {
    const theSource = context.getSource(source, undefined);
    if (theSource.isSeeded()) {
      continue;
    }
    promises.push(theSource.awaitSeeding());
  }

  // We need to await seeding since sources can be ready at different times.
  // If someone queries for Table A in one query then
  // queries for Table A join Table B, Table A will be immediately ready.
  // Since we optimistically run joins, we'll return an incomplete result
  // as Table B hasn't returned from `watch` yet.
  //
  // This waits for all sources to have been loaded into memory
  // before creating the view.
  await Promise.all(promises);

  const pipeline = buildPipeline(
    (sourceName: string, order: Ordering | undefined) =>
      context.getSource(sourceName, order),
    ast,
  );
  const view = new TreeView<Return extends [] ? Return[number] : never>(
    context,
    pipeline as unknown as DifferenceStream<
      Return extends [] ? Return[number] : never
    >,
    makeComparator<Record<string, unknown>>(orderBy[0], orderBy[1]),
    orderBy,
    limit,
  ) as unknown as View<Return extends [] ? Return[number] : Return>;
  view.pullHistoricalData();
  return view;
}

export function fieldsMatch(
  left: readonly Selector[],
  right: readonly Selector[],
) {
  return (
    left.length === right.length &&
    left.every(
      (leftItem, i) =>
        leftItem[0] === right[i][0] && leftItem[1] === right[i][1],
    )
  );
}
