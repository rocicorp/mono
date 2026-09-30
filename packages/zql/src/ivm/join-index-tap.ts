import {unreachable} from '../../../shared/src/asserts.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import type {JoinIndex} from './join-index.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

/**
 * Keeps the parent indexes of a pipeline's EXISTS joins equal to the rows
 * the pipeline emits: rows it yields on fetch or adds on push are indexed,
 * and rows it removes are dropped.
 *
 * An EXISTS join sits upstream of the conditions and the limit, so it sees
 * every parent the pipeline scans, including those a condition rejects or the
 * limit never keeps. Indexing them there would grow with the scan (the whole
 * table for an unlimited or selective EXISTS). This operator is placed after
 * the limit instead, where a row is only seen once it is part of the result,
 * and a row the limit evicts arrives as a remove.
 *
 * It cannot live in the Exists filter: an OR does not evaluate the branches
 * after the first that passes, but every branch's relationship is still
 * synced for the emitted row.
 */
export class JoinIndexTap implements Operator {
  readonly #input: Input;
  readonly #indexes: readonly JoinIndex[];

  #output: Output = throwOutput;

  constructor(input: Input, indexes: readonly JoinIndex[]) {
    this.#input = input;
    this.#indexes = indexes;
    input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  destroy(): void {
    this.#input.destroy();
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    for (const node of this.#input.fetch(req)) {
      if (node !== 'yield') {
        for (const index of this.#indexes) {
          index.add(node.row);
        }
      }
      yield node;
    }
  }

  push(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        for (const index of this.#indexes) {
          index.add(change[ChangeIndex.NODE].row);
        }
        break;
      case ChangeType.REMOVE:
        for (const index of this.#indexes) {
          index.remove(change[ChangeIndex.NODE].row);
        }
        break;
      case ChangeType.EDIT:
        for (const index of this.#indexes) {
          index.remove(change[ChangeIndex.OLD_NODE].row);
          index.add(change[ChangeIndex.NODE].row);
        }
        break;
      case ChangeType.CHILD:
        break;
      default:
        unreachable(change);
    }
    return this.#output.push(change, this);
  }

  *reconcile(_pusher: InputBase): Stream<'yield'> {
    if (this.#output.reconcile) {
      yield* this.#output.reconcile(this);
    }
  }
}
