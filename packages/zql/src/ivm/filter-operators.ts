import {makeEmptyIteratorWithReturn} from '../../../shared/src/iterables.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {BuilderDelegate} from '../builder/builder.ts';
import type {NoSubqueryCondition} from '../builder/filter.ts';
import type {Change} from './change.ts';
import {type Node} from './data.ts';
import type {FetchRequest, Input, InputBase, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {doneResult, type Stream} from './stream.ts';

/**
 * The `where` clause of a ZQL query is implemented using a sub-graph of
 * `FilterOperators`.  This sub-graph starts with a `FilterStart` operator,
 * that adapts from the normal `Operator` `Output`, to the
 * `FilterOperator` `FilterInput`, and ends with a `FilterEnd` operator that
 * adapts from a `FilterOperator` `FilterOutput` to a normal `Operator` `Input`.
 * `FilterOperator`'s do not have `fetch` instead they have a
 * `filter(node: Node): boolean` method.
 * They also have `push` which is just like normal `Operator` push.
 * Not having a `fetch` means these `FilterOperator`'s cannot modify
 * `Node` `row`s or `relationship`s, but they shouldn't, they should just
 * filter.
 *
 * One exception: during a fetch, `Exists` replaces the relationship it read
 * with a function returning the children it read, so that the consumer does
 * not fetch them again. The relationship yields the same nodes, so other
 * branches of a `FanOut`, which see the same node object, are not affected.
 *
 * This `FilterOperator` abstraction enables much more efficient processing of
 * `fetch` for `where` clauses containing OR conditions.
 *
 * See https://github.com/rocicorp/mono/pull/4339
 */

export interface FilterInput extends InputBase {
  /** Tell the input where to send its output. */
  setFilterOutput(output: FilterOutput): void;
}

export interface FilterOutput extends Output {
  // Lets the operator know that we're in a loop of filtering
  // nodes. E.g., so the operator can cache results for the
  // duration of the loop.
  beginFilter(): void;
  filter(node: Node): IterableIterator<'yield', boolean>;
  endFilter(): void;
}

export interface FilterOperator extends FilterInput, FilterOutput {}

/**
 * An implementation of FilterOutput that throws if push or filter is called.
 * It is used as the initial value for for an operator's output before it is
 * set.
 */
export const throwFilterOutput: FilterOutput = {
  // oxlint-disable-next-line require-yield
  *push(_change: Change): Stream<'yield'> {
    throw new Error('Output not set');
  },

  // oxlint-disable-next-line require-yield
  *filter(_node: Node): IterableIterator<'yield', boolean> {
    throw new Error('Output not set');
  },

  reconcile(): Stream<'yield'> {
    throw new Error('Output not set');
  },

  beginFilter() {},
  endFilter() {},
};

export class FilterStart implements FilterInput, Output {
  readonly #input: Input;
  readonly #condition: NoSubqueryCondition | undefined;
  #output: FilterOutput = throwFilterOutput;

  constructor(input: Input, condition?: NoSubqueryCondition) {
    this.#input = input;
    this.#condition = condition;
    input.setOutput(this);
  }

  setFilterOutput(output: FilterOutput) {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  push(change: Change) {
    return this.#output.push(change, this);
  }

  reconcile(_pusher: InputBase): Stream<'yield'> {
    return this.#output.reconcile ? this.#output.reconcile(this) : emptyArray;
  }

  fetch(req: FetchRequest): Stream<Node | 'yield'> {
    const mergedFilter = mergeFilters(req.filter, this.#condition);
    const childReq =
      mergedFilter === req.filter ? req : {...req, filter: mergedFilter};
    return new FilteredNodes(this.#input, childReq, this.#output);
  }
}

/**
 * The nodes of FilterStart's input that pass the filters. An iterator rather
 * than a generator, see `mapNodes` in stream.ts.
 *
 * Nothing happens until the first `next()`, so that `beginFilter` and
 * `endFilter` bracket the iteration, and `endFilter` runs however the
 * iteration ends: at the end of the input, on an error or when the consumer
 * stops early.
 */
class FilteredNodes implements IterableIterator<Node | 'yield'> {
  readonly #input: Input;
  readonly #req: FetchRequest;
  readonly #output: FilterOutput;
  #it: Iterator<Node | 'yield'> | undefined = undefined;
  #begun = false;
  #ended = false;
  // A filter() answer that yielded, and the result holding its node.
  #pending: Iterator<'yield', boolean> | undefined = undefined;
  #pendingResult: IteratorResult<Node | 'yield'> | undefined = undefined;

  constructor(input: Input, req: FetchRequest, output: FilterOutput) {
    this.#input = input;
    this.#req = req;
    this.#output = output;
  }

  [Symbol.iterator](): this {
    return this;
  }

  next(): IteratorResult<Node | 'yield'> {
    if (this.#ended) {
      return doneResult;
    }
    let it = this.#it;
    try {
      if (it === undefined) {
        // A filter that failed to begin is not ended.
        this.#output.beginFilter();
        this.#begun = true;
        it = this.#it = this.#input.fetch(this.#req)[Symbol.iterator]();
      }
      for (;;) {
        let r: IteratorResult<Node | 'yield'>;
        let answer: IteratorResult<'yield', boolean>;
        if (this.#pending !== undefined) {
          // oxlint-disable-next-line typescript/no-non-null-assertion
          r = this.#pendingResult!;
          answer = this.#pending.next();
        } else {
          r = it.next();
          if (r.done) {
            this.#end();
            return r;
          }
          if (r.value === 'yield') {
            return r;
          }
          this.#pending = this.#output.filter(r.value);
          this.#pendingResult = r;
          answer = this.#pending.next();
        }
        if (!answer.done) {
          return yieldResult;
        }
        this.#pending = undefined;
        this.#pendingResult = undefined;
        if (answer.value) {
          return r;
        }
      }
    } catch (e) {
      // As a `for...of` does when its body throws.
      try {
        it?.return?.();
      } finally {
        this.#end();
      }
      throw e;
    }
  }

  return(): IteratorResult<Node | 'yield'> {
    if (!this.#ended) {
      // A filter that has yielded is closed first, then the input, as a
      // generator suspended in `yield* filter(node)` inside its `for...of`
      // would close them.
      const pending = this.#pending;
      this.#pending = undefined;
      this.#pendingResult = undefined;
      try {
        pending?.return?.();
      } finally {
        try {
          this.#it?.return?.();
        } finally {
          this.#end();
        }
      }
    }
    return doneResult;
  }

  #end() {
    if (!this.#ended) {
      this.#ended = true;
      if (this.#begun) {
        this.#output.endFilter();
      }
    }
  }
}

const yieldResult: IteratorResult<Node | 'yield'> = Object.freeze({
  value: 'yield',
  done: false,
});

function mergeFilters(
  reqFilter: NoSubqueryCondition | undefined,
  ownCondition: NoSubqueryCondition | undefined,
): NoSubqueryCondition | undefined {
  if (!ownCondition) {
    return reqFilter;
  }
  if (!reqFilter) {
    return ownCondition;
  }
  return {type: 'and', conditions: [reqFilter, ownCondition]};
}

export class FilterEnd implements Input, FilterOutput {
  readonly #start: FilterStart;
  readonly #input: FilterInput;

  #output: Output = throwFilterOutput;

  constructor(start: FilterStart, input: FilterInput) {
    this.#start = start;
    this.#input = input;
    input.setFilterOutput(this);
  }

  fetch(req: FetchRequest): Stream<Node | 'yield'> {
    return this.#start.fetch(req);
  }

  beginFilter() {}
  endFilter() {}

  filter(_node: Node) {
    return returnTrueEmptyIterator;
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  push(change: Change) {
    return this.#output.push(change, this);
  }

  reconcile(_pusher: InputBase): Stream<'yield'> {
    return this.#output.reconcile ? this.#output.reconcile(this) : emptyArray;
  }
}

const returnTrueEmptyIterator = makeEmptyIteratorWithReturn(true);

export function buildFilterPipeline(
  input: Input,
  delegate: BuilderDelegate,
  pipeline: (filterInput: FilterInput) => FilterInput,
  condition?: NoSubqueryCondition,
): Input {
  const filterStart = new FilterStart(input, condition);
  delegate.addEdge(input, filterStart);
  const middle = pipeline(filterStart);
  delegate.addEdge(filterStart, middle);
  const filterEnd = new FilterEnd(filterStart, middle);
  delegate.addEdge(middle, filterEnd);
  return filterEnd;
}
