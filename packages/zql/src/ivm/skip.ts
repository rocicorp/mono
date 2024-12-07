import {assert} from '../../../shared/src/asserts.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {AddChange, Change, ChildChange, RemoveChange} from './change.js';
import type {Comparator, Node} from './data.js';
import {maybeSplitAndPushEditChange} from './maybe-split-and-push-edit-change.js';
import type {FetchRequest, Input, Operator, Output, Start} from './operator.js';
import type {SourceSchema} from './schema.js';
import type {Stream} from './stream.js';

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

  #output: Output | undefined;

  constructor(input: Input, bound: Bound) {
    this.#input = input;
    this.#bound = bound;
    this.#comparator = input.getSchema().compareRows;
    input.setOutput(this);
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  fetch(req: FetchRequest): Stream<Node> {
    return this.#fetchOrCleanup('fetch', req);
  }

  cleanup(req: FetchRequest): Stream<Node> {
    return this.#fetchOrCleanup('fetch', req);
  }

  *#fetchOrCleanup(method: 'fetch' | 'cleanup', req: FetchRequest) {
    const start = this.#getStart(req);
    if (start === 'empty') {
      return;
    }
    const nodes = this.#input[method]({...req, start});
    if (!req.reverse) {
      yield* nodes;
      return;
    }
    for (const node of nodes) {
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

  push(change: Change): void {
    assert(this.#output, 'Output not set');

    const shouldBePresent = (row: Row) => this.#shouldBePresent(row);
    if (change.type === 'edit') {
      maybeSplitAndPushEditChange(change, shouldBePresent, this.#output);
      return;
    }

    change satisfies AddChange | RemoveChange | ChildChange;

    const changeRow = change.type === 'child' ? change.row : change.node.row;
    if (shouldBePresent(changeRow)) {
      this.#output.push(change);
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
