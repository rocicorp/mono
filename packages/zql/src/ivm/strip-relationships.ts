import type {Writable} from '../../../shared/src/writable.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {
  makeAddChange,
  makeEditChange,
  makeRemoveChange,
  type Change,
} from './change.ts';
import type {Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';

/**
 * Removes relationship payloads from a row stream.
 *
 * Some physical plans use hidden relationships as proof that a filter matched.
 * For example, a flipped EXISTS branch first joins from the child table back to
 * the parent, which naturally attaches the matching child rows to the parent
 * node. If that branch later feeds a root-level union, those child rows are
 * only evidence for the branch. They are not part of the query result.
 *
 * This operator draws that boundary explicitly:
 *
 *   branch proves EXISTS with child rows
 *              |
 *              v
 *   StripRelationships
 *              |
 *              v
 *   plain parent rows enter the union
 */
export class StripRelationships implements Operator {
  readonly #input: Input;
  readonly #schema: SourceSchema;
  #output: Output = throwOutput;

  constructor(input: Input) {
    this.#input = input;
    this.#schema = {
      ...input.getSchema(),
      relationships: {},
    } satisfies Writable<SourceSchema>;
    input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    for (const node of this.#input.fetch(req)) {
      yield node === 'yield' ? node : stripRelationships(node);
    }
  }

  *push(change: Change, _pusher: InputBase): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        yield* this.#output.push(
          makeAddChange(stripRelationships(change[ChangeIndex.NODE])),
          this,
        );
        return;

      case ChangeType.REMOVE:
        yield* this.#output.push(
          makeRemoveChange(stripRelationships(change[ChangeIndex.NODE])),
          this,
        );
        return;

      case ChangeType.EDIT: {
        const oldNode = stripRelationships(change[ChangeIndex.OLD_NODE]);
        const newNode = stripRelationships(change[ChangeIndex.NODE]);
        if (JSON.stringify(oldNode.row) === JSON.stringify(newNode.row)) {
          return;
        }
        yield* this.#output.push(makeEditChange(newNode, oldNode), this);
        return;
      }

      case ChangeType.CHILD:
        return;
    }
  }

  destroy(): void {
    this.#input.destroy();
  }
}

function stripRelationships(node: Node): Node {
  return {
    row: node.row,
    relationships: {},
  };
}
