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
  generateWithOverlay,
  generateWithOverlayUnordered,
  isJoinMatch,
  rowEqualsForCompoundKey,
} from './join-utils.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';

type Args = {
  parent: Input;
  child: Input;
  // The nth key in parentKey corresponds to the nth key in childKey.
  parentKey: CompoundKey;
  childKey: CompoundKey;
  relationshipName: string;
  hidden: boolean;
  system: System;
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

  #output: Output = throwOutput;

  #inprogressChildChange: Change | undefined;
  #inprogressChildChangePosition: Row | undefined;

  constructor({
    parent,
    child,
    parentKey,
    childKey,
    relationshipName,
    hidden,
    system,
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
    });
    child.setOutput({
      push: (change: Change) => this.#pushChild(change),
    });
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
      yield this.#processParentNode(parentNode.row, parentNode.relationships);
    }
  }

  *#pushParent(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
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
    this.#inprogressChildChange = change;
    this.#inprogressChildChangePosition = undefined;
    try {
      const constraint = buildJoinConstraint(
        childRow,
        this.#childKey,
        this.#parentKey,
      );
      if (constraint) {
        for (const parentNode of this.#parent.fetch({constraint})) {
          if (parentNode === 'yield') {
            yield parentNode;
            continue;
          }
          this.#inprogressChildChangePosition = parentNode.row;
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
    } finally {
      this.#inprogressChildChange = undefined;
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
      const stream = constraint ? this.#child.fetch({constraint}) : [];

      if (
        this.#inprogressChildChange &&
        isJoinMatch(
          parentNodeRow,
          this.#parentKey,
          this.#inprogressChildChange[ChangeIndex.NODE].row,
          this.#childKey,
        ) &&
        this.#inprogressChildChangePosition &&
        this.#schema.compareRows(
          parentNodeRow,
          this.#inprogressChildChangePosition,
        ) > 0
      ) {
        const childSchema = this.#child.getSchema();
        if (childSchema.sort === undefined) {
          return generateWithOverlayUnordered(
            stream,
            this.#inprogressChildChange,
            childSchema,
          );
        }
        return generateWithOverlay(
          stream,
          this.#inprogressChildChange,
          childSchema,
        );
      }
      return stream;
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
