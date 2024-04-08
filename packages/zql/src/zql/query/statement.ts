import {Entity} from '../../generate.js';
import {buildPipeline, orderingProp} from '../ast-to-ivm/pipeline-builder.js';
import {AST, Primitive} from '../ast/ast.js';
import {Context} from '../context/context.js';
import {invariant, must} from '../error/asserts.js';
import {compareEntityFields} from '../ivm/compare.js';
import {DifferenceStream} from '../ivm/graph/difference-stream.js';
import {MutableTreeView} from '../ivm/view/tree-view.js';
import {View} from '../ivm/view/view.js';
import {MakeHumanReadable} from './entity-query.js';

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
        context.getSource(sourceName, this.#ast.orderBy)
          .stream as unknown as DifferenceStream<T>,
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
        this.#ast.orderBy[1] === 'asc' ? ascComparator : descComparator,
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

export function ascComparator<T extends {[orderingProp]: Primitive[]}>(
  l: T,
  r: T,
): number {
  const leftVals = l[orderingProp];
  const rightVals = r[orderingProp];

  invariant(
    leftVals.length === rightVals.length,
    'orderingProp lengths must match',
  );

  let comp = 0;
  for (let i = 0; i < leftVals.length; i++) {
    const leftVal = leftVals[i];
    const rightVal = rightVals[i];
    comp = compareEntityFields(leftVal, rightVal);
    if (comp !== 0) {
      return comp;
    }
  }

  return comp;
}

export function descComparator<T extends {[orderingProp]: Primitive[]}>(
  l: T,
  r: T,
): number {
  return ascComparator(r, l);
}
