import {assert} from 'shared/src/asserts.js';
import {Change} from './change.js';
import {Comparator, Node, Row} from './data.js';
import {FetchRequest, Input, Operator, Output, Start} from './operator.js';
import {Schema} from './schema.js';
import {Stream} from './stream.js';

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
    this.#input.setOutput(this);
  }

  getSchema(): Schema {
    return this.#input.getSchema();
  }

  fetch(req: FetchRequest): Stream<Node> {
    return this.#input.fetch({...req, start: this.#getStart(req)});
  }

  cleanup(req: FetchRequest): Stream<Node> {
    return this.#input.cleanup({...req, start: this.#getStart(req)});
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  push(change: Change): void {
    if (!this.#output) {
      return;
    }

    const changeRow = change.type === 'child' ? change.row : change.node.row;
    const cmp = this.#comparator(this.#bound.row, changeRow);
    if (cmp > 0) {
      return;
    }
    if (cmp === 0 && this.#bound.exclusive) {
      return;
    }

    this.#output.push(change);
  }

  #getStart(req: FetchRequest): Start | undefined {
    const boundStart = {
      row: this.#bound.row,
      basis: this.#bound.exclusive ? 'after' : 'at',
    } as const;

    if (!req.start) {
      return boundStart;
    }

    const cmp = this.#comparator(this.#bound.row, req.start.row);

    // The skip bound is after the requested bound. The requested bound cannot
    // be relevant because even if it was basis: 'after', the skip bound is
    // itself after the requested bound. Return the skip bound.
    if (cmp > 0) {
      return boundStart;
    }

    // The skip bound and requested bound are equal. If either is exclusive,
    // return that bound with ecxlusive. Otherwise, return the skip bound.
    // There is the case where the requested bound is basis: 'before', but
    // that cannot be relevant.
    if (cmp === 0) {
      if (this.#bound.exclusive || req.start.basis === 'after') {
        return {
          row: this.#bound.row,
          basis: 'after',
        };
      }
      return boundStart;
    }

    assert(cmp < 0);

    // The skip bound is before the requested bound. If the requested bound is
    // either 'at' or 'after', the skip bound cannot be relevant. Return the
    // requested bound.
    if (req.start.basis === 'at' || req.start.basis === 'after') {
      return req.start;
    }

    // That leaves the one interesting case: the skip bound is before the
    // requested bound, but the requested bound is basis: 'before'. It is
    // possible that the first element before the requested bound is itself the
    // skip bound, or there could be some other element between. We'll have to
    // fetch to find out.
    req.start.basis satisfies 'before';

    const [node] = this.#input.fetch(req) as Array<Node | undefined>;

    // There's no element at all before the requested bound, not even the skip
    // bound. In this case we may as well return the requested bound with 'at'
    // to simplify work for the source.
    if (!node) {
      return {
        row: req.start.row,
        basis: 'at',
      };
    }

    // If there's an element before the requested bound, but it's before the
    // skip bound, then we'll use the skip bound afterall.
    if (this.#comparator(node.row, this.#bound.row) <= 0) {
      return boundStart;
    }

    // Finally, if there's an element before the requested bound, and it's after
    // the skip bound, then that's the element we should start at.
    return {
      row: node.row,
      basis: 'at',
    };
  }
}
