import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey, System} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {
  makeAddChange,
  makeChildChange,
  makeEditChange,
  makeRemoveChange,
  type Change,
} from './change.ts';
import type {Node} from './data.ts';
import {
  buildJoinConstraint,
  JoinIndex,
  rowEqualsForCompoundKey,
  type JoinStorage,
} from './join-utils.ts';
import {mergeSortedStreams} from './memory-source.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
  type Storage,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';
import {type TakeBoundProvider} from './take-gate.ts';

type Args = {
  parent: Input;
  child: Input;
  // The nth key in parentKey corresponds to the nth key in childKey.
  parentKey: CompoundKey;
  childKey: CompoundKey;
  relationshipName: string;
  hidden: boolean;
  system: System;
  parentPartitionKey?: CompoundKey | undefined;
  boundProvider?: TakeBoundProvider | undefined;
  storage: Storage;
};

/**
 * The Join operator joins the output from two upstream inputs. Zero's join
 * is a little different from SQL's join in that we output hierarchical data,
 * not a flat table. This makes it a lot more useful for UI programming and
 * avoids duplicating tons of data like left join would.
 *
 * The Nodes output from Join have a new relationship added to them, which has
 * the name #relationshipName. The value of the relationship is a stream of
 * child nodes which are the corresponding values from the child source.
 */
export class Join implements Input {
  readonly #parent: Input;
  readonly #child: Input;
  readonly #parentKey: CompoundKey;
  readonly #childKey: CompoundKey;
  readonly #relationshipName: string;
  readonly #schema: SourceSchema;
  readonly #joinIndex: JoinIndex;

  #output: Output = throwOutput;

  constructor({
    parent,
    child,
    parentKey,
    childKey,
    relationshipName,
    hidden,
    system,
    parentPartitionKey,
    storage,
  }: Args) {
    assert(parent !== child, 'Parent and child must be different operators');
    assert(
      parentKey.length === childKey.length,
      'The parentKey and childKey keys must have same length',
    );
    this.#parent = parent;
    this.#child = child;
    this.#parentKey = parentKey;
    this.#childKey = childKey;
    this.#relationshipName = relationshipName;
    this.#joinIndex = new JoinIndex(
      storage as unknown as JoinStorage,
      parentKey,
      parent.getSchema().primaryKey,
      parentPartitionKey,
    );

    const parentSchema = parent.getSchema();
    const childSchema = child.getSchema();
    this.#schema = {
      ...parentSchema,
      relationships: {
        ...parentSchema.relationships,
        [relationshipName]: {
          ...childSchema,
          isHidden: hidden,
          system,
        },
      },
    };

    parent.setOutput({
      push: (change: Change) => this.#pushParent(change),
      reconcile: () => this.#reconcile(),
    });
    child.setOutput({
      push: (change: Change) => this.#pushChild(change),
      reconcile: () => this.#reconcile(),
    });
  }

  *#reconcile(): Stream<'yield'> {
    if (this.#output.reconcile) {
      yield* this.#output.reconcile(this);
    }
  }

  destroy(): void {
    this.#parent.destroy();
    this.#child.destroy();
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    for (const parentNode of this.#parent.fetch(req)) {
      if (parentNode === 'yield') {
        yield parentNode;
        continue;
      }
      this.#joinIndex.index(parentNode.row);
      yield this.#processParentNode(parentNode.row, parentNode.relationships);
    }
  }

  *#pushParent(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        this.#joinIndex.index(change[ChangeIndex.NODE].row);
        yield* this.#output.push(
          makeAddChange(
            this.#processParentNode(
              change[ChangeIndex.NODE].row,
              change[ChangeIndex.NODE].relationships,
            ),
          ),
          this,
        );
        break;
      case ChangeType.REMOVE:
        this.#joinIndex.unindex(change[ChangeIndex.NODE].row);
        yield* this.#output.push(
          makeRemoveChange(
            this.#processParentNode(
              change[ChangeIndex.NODE].row,
              change[ChangeIndex.NODE].relationships,
            ),
          ),
          this,
        );
        break;
      case ChangeType.CHILD:
        yield* this.#output.push(
          makeChildChange(
            this.#processParentNode(
              change[ChangeIndex.NODE].row,
              change[ChangeIndex.NODE].relationships,
            ),
            change[ChangeIndex.CHILD_DATA],
          ),
          this,
        );
        break;
      case ChangeType.EDIT: {
        // Assert the edit could not change the relationship.
        assert(
          rowEqualsForCompoundKey(
            change[ChangeIndex.OLD_NODE].row,
            change[ChangeIndex.NODE].row,
            this.#parentKey,
          ),
          `Parent edit must not change relationship.`,
        );
        this.#joinIndex.unindex(change[ChangeIndex.OLD_NODE].row);
        this.#joinIndex.index(change[ChangeIndex.NODE].row);
        yield* this.#output.push(
          makeEditChange(
            this.#processParentNode(
              change[ChangeIndex.NODE].row,
              change[ChangeIndex.NODE].relationships,
            ),
            this.#processParentNode(
              change[ChangeIndex.OLD_NODE].row,
              change[ChangeIndex.OLD_NODE].relationships,
            ),
          ),
          this,
        );
        break;
      }
      default:
        unreachable(change);
    }
  }

  *#pushChild(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
      case ChangeType.REMOVE:
        yield* this.#pushChildChange(change[ChangeIndex.NODE].row, change);
        break;
      case ChangeType.CHILD:
        yield* this.#pushChildChange(change[ChangeIndex.NODE].row, change);
        break;
      case ChangeType.EDIT: {
        const childRow = change[ChangeIndex.NODE].row;
        const oldChildRow = change[ChangeIndex.OLD_NODE].row;
        // Assert the edit could not change the relationship.
        assert(
          rowEqualsForCompoundKey(oldChildRow, childRow, this.#childKey),
          'Child edit must not change relationship.',
        );
        yield* this.#pushChildChange(childRow, change);
        break;
      }

      default:
        unreachable(change);
    }
  }

  *#pushChildChange(childRow: Row, change: Change): Stream<'yield'> {
    const constraint = buildJoinConstraint(
      childRow,
      this.#childKey,
      this.#parentKey,
    );
    if (!constraint) {
      return;
    }
    const matching = this.#joinIndex.getMatching(childRow, this.#childKey);
    if (!matching) {
      return;
    }

    const fetchConstraints = matching.map(entry =>
      entry.partitionConstraint
        ? {...constraint, ...entry.partitionConstraint}
        : constraint,
    );

    let parentNodeStream: Stream<Node | 'yield'>;
    if (fetchConstraints.length === 1) {
      parentNodeStream = this.#parent.fetch({
        constraint: fetchConstraints[0],
      });
    } else {
      const streams = fetchConstraints.map(c =>
        this.#parent.fetch({constraint: c}),
      );
      const compare = (a: Node, b: Node) =>
        this.#schema.compareRows(a.row, b.row);
      parentNodeStream = mergeSortedStreams(streams, compare);
    }

    for (const parentNode of parentNodeStream) {
      if (parentNode === 'yield') {
        yield parentNode;
        continue;
      }
      const childChange = makeChildChange(
        this.#processParentNode(parentNode.row, parentNode.relationships),
        {
          relationshipName: this.#relationshipName,
          change,
        },
      );
      yield* this.#output.push(childChange, this);
    }
  }

  #processParentNode(
    parentNodeRow: Row,
    parentNodeRelations: Record<string, () => Stream<Node | 'yield'>>,
  ): Node {
    const childStream = () => {
      const constraint = buildJoinConstraint(
        parentNodeRow,
        this.#parentKey,
        this.#childKey,
      );
      return constraint ? this.#child.fetch({constraint}) : [];
    };

    return {
      row: parentNodeRow,
      relationships: {
        ...parentNodeRelations,
        [this.#relationshipName]: childStream,
      },
    };
  }
}
