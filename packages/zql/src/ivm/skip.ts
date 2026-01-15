import type {Row} from '../../../zero-protocol/src/data.ts';
import type {AddChange, Change, ChildChange, RemoveChange} from './change.ts';
import type {Comparator, Node} from './data.ts';
import {maybeSplitAndPushEditChange} from './maybe-split-and-push-edit-change.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
  type Start,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

export type Bound = {
  row: Row;
  exclusive: boolean;
};

/**
 * Skip sets the start position for the pipeline. No rows before the bound will
 * be output.
 */
export class Skip implements Operator {
  readonly #input: Input;
  readonly #bound: Bound;
  readonly #comparator: Comparator;

  #output: Output = throwOutput;

  constructor(input: Input, bound: Bound) {
    this.#input = input;
    this.#bound = bound;
    this.#comparator = input.getSchema().compareRows;
    input.setOutput(this);
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    const start = this.#getStart(req);
    if (start === 'empty') {
      return;
    }
    const nodes = this.#input.fetch({...req, start});
    if (!req.reverse) {
      yield* nodes;
      return;
    }
    for (const node of nodes) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      if (!this.#shouldBePresent(node.row)) {
        return;
      }
      yield node;
    }
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  #shouldBePresent(row: Row): boolean {
    const cmp = this.#comparator(this.#bound.row, row);
    return cmp < 0 || (cmp === 0 && !this.#bound.exclusive);
  }

  *push(change: Change): Stream<'yield'> {
    const shouldBePresent = (row: Row) => this.#shouldBePresent(row);
    if (change.type === 'edit') {
      yield* maybeSplitAndPushEditChange(
        change,
        shouldBePresent,
        this.#output,
        this,
      );
      return;
    }

    change satisfies AddChange | RemoveChange | ChildChange;

    if (shouldBePresent(change.node.row)) {
      yield* this.#output.push(change, this);
    }
  }

  #getStart(req: FetchRequest): Start | undefined | 'empty' {
    const boundStart = {
      row: this.#bound.row,
      basis: this.#bound.exclusive ? 'after' : 'at',
    } as const;

    if (!req.start) {
      if (req.reverse) {
        return undefined;
      }
      return boundStart;
    }

    const cmp = this.#comparator(this.#bound.row, req.start.row);

    if (!req.reverse) {
      // The skip bound is after the requested bound. The requested bound cannot
      // be relevant because even if it was basis: 'after', the skip bound is
      // itself after the requested bound. Return the skip bound.
      if (cmp > 0) {
        return boundStart;
      }

      // The skip bound and requested bound are equal. If either is exclusive,
      // return that bound with exclusive. Otherwise, return the skip bound.
      if (cmp === 0) {
        if (this.#bound.exclusive || req.start.basis === 'after') {
          return {
            row: this.#bound.row,
            basis: 'after',
          };
        }
        return boundStart;
      }

      return req.start;
    }

    req.reverse satisfies true;

    // bound is after the start, but request is for reverse so results
    // must be empty
    if (cmp > 0) {
      return 'empty';
    }

    if (cmp === 0) {
      // if both are inclusive, the result can be the single row at bound
      // return it as start
      if (!this.#bound.exclusive && req.start.basis === 'at') {
        return boundStart;
      }
      // otherwise the results must be empty, one or both are exclusive
      // in opposite directions
      return 'empty';
    }

    // bound is before the start, return start
    return req.start;
  }
}
