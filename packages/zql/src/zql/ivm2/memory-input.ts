import BTree from 'btree';
import type {Input, Output, Request} from './operator.js';
import {makeComparator, type Comparator, type Row} from './data.js';
import type {Ordering} from '../ast2/ast.js';
import type {Change} from './tree-diff.js';
import {assertNotNull} from 'shared/src/asserts.js';
import {ChangeStream} from './change-stream.js';

/**
 * A `MemoryInput` is an input that provides data to the pipeline from
 * an in-memory data source.
 *
 * This data is kept in sorted order as downstream pipelines will
 * always expect the data they receive from `pull` to be in sorted order.
 */
export class MemoryInput implements Input {
  readonly comparator: Comparator;

  #tree: BTree<Row, undefined>;
  #output: Output | null = null;

  constructor(order: Ordering) {
    // TODO(aa): This does not have the correct semantics at least so far as
    // data.ts is concerned. According to that, the non-id fields should not
    // matter for equality but here they do since we are using the sort at the
    // comparator.
    //
    // I think in order to make the semantics correct, we need to have the
    // "canonical" order source, as ivm1 did, then treat the alternate sorts
    // as indexes.
    //
    // We need the canonical source to honor the request constraints in many
    // cases anyway.
    this.comparator = makeComparator(order);
    this.#tree = new BTree(undefined, this.comparator);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  push(changes: Iterable<Change>) {
    assertNotNull(this.#output);
    this.#output.push(this, {
      sorted: false,
      changes: new ChangeStream(this.#applyChanges(changes), 'needy'),
    });
  }

  *#applyChanges(changes: Iterable<Change>) {
    for (const change of changes) {
      if (change.type === 'add') {
        this.#tree = this.#tree.with(change.row, undefined);
        yield change;
      } else {
        yield change;
        this.#tree = this.#tree.without(change.row);
      }
    }
  }

  pull(_req: Request) {
    return {
      appliedFilters: [],
      diff: {
        sorted: true,
        changes: new ChangeStream(this.#pullChanges(), 'normal'),
      },
    };
  }

  *#pullChanges() {
    for (const row of this.#tree.keys()) {
      yield {
        type: 'add' as const,
        row,
      };
    }
  }
}
