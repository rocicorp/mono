import type {BuilderDelegate} from '../builder/builder.ts';
import type {NoSubqueryCondition} from '../builder/filter.ts';
import type {Change} from './change.ts';
import {type Node} from './data.ts';
import {
  type FetchRequest,
  type Input,
  type InputBase,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {LazyPullStream, type PullStream, type Stream} from './stream.ts';

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
  /**
   * The verdict, or 'yield' to hand control back -- the caller must then call
   * again with the same node until it gets a boolean. Delegates that never
   * suspend return the boolean directly, so a chain of them costs no
   * allocation per node. `Exists` is the one that does suspend, and holds its
   * position in explicit state rather than in a generator.
   */
  filter(node: Node): boolean | 'yield';
  endFilter(): void;
}

export interface FilterOperator extends FilterInput, FilterOutput {}

/**
 * An implementation of FilterOutput that throws if push or filter is called.
 * It is used as the initial value for for an operator's output before it is
 * set.
 */
export const throwFilterOutput: FilterOutput = {
  push(_change: Change): Stream<'yield'> {
    throw new Error('Output not set');
  },

  filter(): boolean | 'yield' {
    throw new Error('Output not set');
  },

  beginFilter() {},
  endFilter() {},
};

export class FilterStart implements FilterInput, Output {
  readonly #input: Input;
  readonly #condition: NoSubqueryCondition | undefined;
  #output: FilterOutput = throwFilterOutput;

  constructor(input: Input, condition?: NoSubqueryCondition | undefined) {
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

  push(change: Change): Stream<'yield'> {
    return this.#output.push(change, this);
  }

  fetch(req: FetchRequest): PullStream<Node | 'yield'> {
    const mergedFilter = mergeFilters(req.filter, this.#condition);
    const childReq =
      mergedFilter === req.filter ? req : {...req, filter: mergedFilter};
    // Lazy so beginFilter() runs when iteration starts, as the generator did.
    return new LazyPullStream(() => {
      this.#output.beginFilter();
      return new FilterStartPull(this.#input.fetch(childReq), this.#output);
    });
  }
}

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

  fetch(req: FetchRequest): PullStream<Node | 'yield'> {
    return this.#start.fetch(req);
  }

  beginFilter() {}
  endFilter() {}

  filter(_node: Node): boolean {
    return true;
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

  push(change: Change): Stream<'yield'> {
    return this.#output.push(change, this);
  }
}

export function buildFilterPipeline(
  input: Input,
  delegate: BuilderDelegate,
  pipeline: (filterInput: FilterInput) => FilterInput,
  condition?: NoSubqueryCondition | undefined,
): Input {
  const filterStart = new FilterStart(input, condition);
  delegate.addEdge(input, filterStart);
  const middle = pipeline(filterStart);
  delegate.addEdge(filterStart, middle);
  const filterEnd = new FilterEnd(filterStart, middle);
  delegate.addEdge(middle, filterEnd);
  return filterEnd;
}

/**
 * FilterStart's fetch in the pull protocol. Holds the node a delegate has
 * suspended on so the same node is offered again after a 'yield'; calls
 * endFilter() exactly once, on exhaustion, close, or throw.
 */
class FilterStartPull implements PullStream<Node | 'yield'> {
  readonly #input: PullStream<Node | 'yield'>;
  readonly #output: FilterOutput;
  #pending: Node | undefined;
  #ended = false;

  constructor(input: PullStream<Node | 'yield'>, output: FilterOutput) {
    this.#input = input;
    this.#output = output;
  }

  next(): Node | 'yield' | undefined {
    if (this.#ended) {
      return undefined;
    }
    try {
      for (;;) {
        let node: Node;
        const pending = this.#pending;
        if (pending !== undefined) {
          node = pending;
        } else {
          const v = this.#input.next();
          if (v === undefined) {
            this.#end();
            return undefined;
          }
          if (v === 'yield') {
            return v;
          }
          node = v;
        }
        const verdict = this.#output.filter(node);
        if (verdict === 'yield') {
          this.#pending = node;
          return 'yield';
        }
        this.#pending = undefined;
        if (verdict) {
          return node;
        }
      }
    } catch (e) {
      this.#input.close();
      this.#end();
      throw e;
    }
  }

  #end(): void {
    if (!this.#ended) {
      this.#ended = true;
      this.#output.endFilter();
    }
  }

  close(): void {
    if (!this.#ended) {
      this.#input.close();
      this.#end();
    }
  }
}
