import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey, System} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
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
import {JoinIndex} from './join-index.ts';
import {
  buildJoinConstraint,
  canonicalKey,
  generateWithOverlay,
  generateWithOverlayUnordered,
  isJoinMatch,
  rowEqualsForCompoundKey,
} from './join-utils.ts';
import {mergeSortedStreams} from './memory-source.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';
import {
  isInParentFetch,
  readParentFetchBounds,
  type ParentFetchBound,
  type TakeBoundProvider,
} from './take-gate.ts';

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
  /**
   * Set when this join feeds an EXISTS or NOT EXISTS condition. Its parent
   * index is then kept by a JoinIndexTap at the end of the pipeline, so it
   * only holds the parents the pipeline emits, not every parent this join
   * sees. A child change that misses the index is dropped unless it could
   * make a parent pass the condition: an add for EXISTS, a remove for NOT
   * EXISTS. Those still fetch the parents.
   */
  exists?: ExistsJoin | undefined;
};

export type ExistsJoin = {
  readonly op: 'EXISTS' | 'NOT EXISTS';
  readonly parentIndex: JoinIndex;
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
  readonly #parentIndex: JoinIndex;
  /**
   * Whether this join adds and removes parents in #parentIndex itself. Not
   * so for EXISTS joins, see Args.exists.
   */
  readonly #indexesParents: boolean;
  /**
   * The child change type that is still pushed to the parents when no
   * indexed parent has its join key. See Args.exists.
   */
  readonly #qualifyingChildChange: ChangeType | undefined;
  readonly #boundProvider: TakeBoundProvider | undefined;

  #output: Output = throwOutput;

  #inprogressChildChange: Change | undefined;
  #inprogressChildChangePosition: Row | undefined;
  /**
   * Primary keys of the parents #inprogressChildChange has reached so far,
   * kept only when the parent input is unordered. An unordered stream is not
   * in `compareRows` order (SQLite returns it in whatever order its plan
   * visits, e.g. rowid order), so whether a parent is still in the push queue
   * cannot be decided by comparing it to #inprogressChildChangePosition.
   */
  #inprogressReachedParents: Set<string> | undefined;
  #inprogressParentFetchBounds: ParentFetchBound[] | undefined;

  constructor({
    parent,
    child,
    parentKey,
    childKey,
    relationshipName,
    hidden,
    system,
    parentPartitionKey,
    boundProvider,
    exists,
  }: Args) {
    assert(parent !== child, 'Parent and child must be different operators');
    assert(
      parentKey.length === childKey.length,
      'The parentKey and childKey keys must have same length',
    );
    assert(!exists || !parentPartitionKey, 'EXISTS joins are not partitioned');
    this.#parent = parent;
    this.#child = child;
    this.#parentKey = parentKey;
    this.#childKey = childKey;
    this.#relationshipName = relationshipName;
    if (exists) {
      this.#parentIndex = exists.parentIndex;
      this.#indexesParents = false;
      this.#qualifyingChildChange =
        exists.op === 'EXISTS' ? ChangeType.ADD : ChangeType.REMOVE;
    } else {
      this.#parentIndex = new JoinIndex(
        parentKey,
        parent.getSchema().primaryKey,
        parentPartitionKey,
      );
      this.#indexesParents = true;
      this.#qualifyingChildChange = undefined;
    }
    this.#boundProvider = boundProvider;

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

  get parentIndexForTest(): JoinIndex {
    return this.#parentIndex;
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
      this.#indexParentRow(parentNode.row);
      yield this.#processParentNode(parentNode.row, parentNode.relationships);
    }
  }

  *#pushParent(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        this.#indexParentRow(change[ChangeIndex.NODE].row);
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
        this.#unindexParentRow(change[ChangeIndex.NODE].row);
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
        this.#unindexParentRow(change[ChangeIndex.OLD_NODE].row);
        this.#indexParentRow(change[ChangeIndex.NODE].row);
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
    this.#inprogressReachedParents =
      this.#parent.getSchema().sort === undefined ? new Set() : undefined;
    try {
      const constraint = buildJoinConstraint(
        childRow,
        this.#childKey,
        this.#parentKey,
      );
      if (constraint) {
        const partitions = this.#parentIndex.lookup(childRow, this.#childKey);
        let fetchConstraints: Record<string, Value>[];
        if (partitions) {
          fetchConstraints = partitions.map(partitionConstraint =>
            partitionConstraint
              ? {...constraint, ...partitionConstraint}
              : constraint,
          );
        } else if (change[ChangeIndex.TYPE] === this.#qualifyingChildChange) {
          fetchConstraints = [constraint];
        } else {
          return;
        }

        if (this.#boundProvider) {
          this.#inprogressParentFetchBounds = readParentFetchBounds(
            this.#boundProvider,
            fetchConstraints,
          );
        }

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
          this.#inprogressChildChangePosition = parentNode.row;
          this.#inprogressReachedParents?.add(
            canonicalKey(parentNode.row, this.#schema.primaryKey),
          );
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
      this.#inprogressChildChangePosition = undefined;
      this.#inprogressReachedParents = undefined;
      this.#inprogressParentFetchBounds = undefined;
    }
  }

  /**
   * Whether the in-progress child change has yet to reach `parentNodeRow`,
   * i.e. the row comes after #inprogressChildChangePosition in the parent
   * stream.
   */
  #isAfterInprogressPosition(parentNodeRow: Row): boolean {
    if (this.#inprogressChildChangePosition === undefined) {
      return false;
    }
    if (this.#inprogressReachedParents) {
      return !this.#inprogressReachedParents.has(
        canonicalKey(parentNodeRow, this.#schema.primaryKey),
      );
    }
    return (
      this.#schema.compareRows(
        parentNodeRow,
        this.#inprogressChildChangePosition,
      ) > 0
    );
  }

  #indexParentRow(row: Row): void {
    if (this.#indexesParents) {
      this.#parentIndex.add(row);
    }
  }

  #unindexParentRow(row: Row): void {
    if (this.#indexesParents) {
      this.#parentIndex.remove(row);
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

      // The parent has yet to get the in-progress child change if it comes
      // after the current position and a parent fetch of the push yields it.
      // With a TakeGate the fetches are capped at the bounds read when they
      // started.
      const inPushQueue =
        this.#isAfterInprogressPosition(parentNodeRow) &&
        (this.#inprogressParentFetchBounds === undefined ||
          isInParentFetch(
            this.#inprogressParentFetchBounds,
            parentNodeRow,
            this.#schema.compareRows,
          ));

      if (
        this.#inprogressChildChange &&
        isJoinMatch(
          parentNodeRow,
          this.#parentKey,
          this.#inprogressChildChange[ChangeIndex.NODE].row,
          this.#childKey,
        ) &&
        inPushQueue
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
