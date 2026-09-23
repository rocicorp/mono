import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {binarySearch} from '../../../shared/src/binary-search.ts';
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
import {constraintsAreCompatible, type Constraint} from './constraint.ts';
import type {Node} from './data.ts';
import {
  buildJoinConstraint,
  canonicalKey,
  canonicalKeyForTest,
  generateWithOverlayNoYield,
  getMatchingParentEntries,
  indexParentInStorage,
  isJoinMatch,
  rowEqualsForCompoundKey,
  unindexParentInStorage,
  type JoinStorage,
} from './join-utils.ts';
import {mergeSortedStreams} from './memory-source.ts';
import {MemoryStorage} from './memory-storage.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type MultiConstraint,
  type Output,
  type Storage,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';
import type {TakeBoundProvider} from './take-gate.ts';

/**
 * Maximum number of entries sent in a single batched `parent.fetch`
 * call. Larger child-node sets are split into multiple fetches whose
 * sorted streams are merged in JS.
 *
 * Why bound this:
 *  - **Bounded fetch on early termination.** `mergeSortedStreams` primes
 *    one row from every chunk before yielding the first output, so all
 *    chunks open their cursors up front. Smaller chunks cap the
 *    worst-case overfetch when downstream `Take` consumes only a few
 *    rows — at chunk N, we may waste up to ~N index seeks before
 *    `.return()` propagates.
 *  - **Parameter limit.** Well under SQLite's default
 *    `SQLITE_MAX_VARIABLE_NUMBER` (32766). Compound keys multiply the
 *    parameter count by key length, so we leave headroom.
 *
 * Tested 64/128/256; 256 had the best worst-case across planner suites
 * and perf tests.
 *
 * We should, however, start doing shadow tests of the planner
 * against cloudzero queries.
 */
const MULTI_CONSTRAINT_CHUNK_SIZE = 256;

// Mutable test seam — production code reads this via the getter.
let multiConstraintChunkSize: number = MULTI_CONSTRAINT_CHUNK_SIZE;

export function getMultiConstraintChunkSize(): number {
  return multiConstraintChunkSize;
}

/** Test only. Returns a restore function. */
export function setMultiConstraintChunkSizeForTest(size: number): () => void {
  const prev = multiConstraintChunkSize;
  multiConstraintChunkSize = size;
  return () => {
    multiConstraintChunkSize = prev;
  };
}

type Args = {
  parent: Input;
  child: Input;
  // The nth key in childKey corresponds to the nth key in parentKey.
  parentKey: CompoundKey;
  childKey: CompoundKey;

  relationshipName: string;
  hidden: boolean;
  system: System;
  parentPartitionKey?: CompoundKey | undefined;
  boundProvider?: TakeBoundProvider | undefined;
  trackPartitions?: boolean | undefined;
  storage?: Storage | undefined;
};

/**
 * An *inner* join which fetches nodes from its child input first and then
 * fetches their related nodes from its parent input.  Output nodes are the
 * nodes from parent input (in parent input order), which have at least one
 * related child.  These output nodes have a new relationship added to them,
 * which has the name `relationshipName`. The value of the relationship is a
 * stream of related nodes from the child input (in child input order).
 */
export class FlippedJoin implements Input {
  readonly #parent: Input;
  readonly #child: Input;
  readonly #parentKey: CompoundKey;
  readonly #childKey: CompoundKey;
  readonly #relationshipName: string;
  readonly #schema: SourceSchema;
  readonly #parentPartitionKey: CompoundKey | undefined;
  readonly #boundProvider: TakeBoundProvider | undefined;
  readonly #storage: JoinStorage | undefined;

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
    parentPartitionKey,
    boundProvider,
    trackPartitions,
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
    this.#parentPartitionKey = parentPartitionKey;
    this.#boundProvider = boundProvider;
    this.#storage =
      (trackPartitions ?? true)
        ? ((storage ?? new MemoryStorage()) as unknown as JoinStorage)
        : undefined;

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
    this.#child.destroy();
    this.#parent.destroy();
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    // Translate constraints for the parent on parts of the join key to
    // constraints for the child.
    const childConstraint: Record<string, Value> = {};
    let hasChildConstraint = false;
    if (req.constraint) {
      for (const [key, value] of Object.entries(req.constraint)) {
        const index = this.#parentKey.indexOf(key);
        if (index !== -1) {
          hasChildConstraint = true;
          childConstraint[this.#childKey[index]] = value;
        }
      }
    }

    const childNodes: Node[] = [];
    for (const node of this.#child.fetch(
      hasChildConstraint ? {constraint: childConstraint} : {},
    )) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      childNodes.push(node);
    }

    // FlippedJoin's split-push change overlay logic is largely
    // the same as Join's with the exception of remove.  For remove,
    // the change is undone here, and then re-applied to parents with order
    // less than or equal to change.position below.  This is necessary
    // because if the removed node was the last related child, the
    // related parents with position greater than change.position
    // (which should not yet have the node removed), would not even
    // be fetched here, and would be absent from the output all together.
    if (this.#inprogressChildChange?.[ChangeIndex.TYPE] === ChangeType.REMOVE) {
      const removedNode = this.#inprogressChildChange[ChangeIndex.NODE];
      const compare = this.#child.getSchema().compareRows;
      const insertPos = binarySearch(childNodes.length, i =>
        compare(removedNode.row, childNodes[i].row),
      );
      childNodes.splice(insertPos, 0, removedNode);
    }

    yield* this.#fetchBatched(req, childNodes);
  }

  /**
   * Fetches parents for `childNodes` in batched calls, using
   * `multiConstraint` so the source can issue one query per chunk (e.g.
   * SQL `IN` with index-aware seek) instead of N per-child cursors.
   *
   * Multi-constraint values are split into chunks of `CHUNK_SIZE`, so
   * SQL `IN` lists stay bounded — predictable plans, statement-cache
   * hits across calls of the same chunk size, well below SQLite's
   * parameter limit.
   *
   * Within each chunk, the source returns parents in `compareRows` order.
   * Across chunks, we merge with `mergeSortedStreams` so the overall
   * stream is also in order. Note: the merge primes one row from every
   * chunk before yielding the first output, so all chunks open their
   * cursors up front. Early termination downstream then prevents any
   * further work on un-advanced chunks (cursors get `.return()`'d via
   * `mergeSortedStreams`'s finally block).
   *
   * Replaces the previous split between `#fetchMergeSort` and
   * `#fetchQuicksort`. The unique-vs-not distinction is no longer needed:
   * the source handles cardinality (single index seek for each value) and
   * ordering (SQL `ORDER BY` / index walk).
   */
  *#fetchBatched(
    req: FetchRequest,
    childNodes: Node[],
  ): Stream<Node | 'yield'> {
    const parentReqConstraint = req.constraint;
    const parentKey = this.#parentKey;
    const childKey = this.#childKey;

    // Build (deduped) multi-constraint and a key→child-indexes map. Same
    // parent-key value across multiple children groups them together.
    const computedMulti: Constraint[] = [];
    const childIndexesByKey = new Map<string, number[]>();
    for (let i = 0; i < childNodes.length; i++) {
      const constraintFromChild = buildJoinConstraint(
        childNodes[i].row,
        childKey,
        parentKey,
      );
      if (
        !constraintFromChild ||
        (parentReqConstraint &&
          !constraintsAreCompatible(constraintFromChild, parentReqConstraint))
      ) {
        continue;
      }
      const key = canonicalKey(constraintFromChild, parentKey);
      const existing = childIndexesByKey.get(key);
      if (existing === undefined) {
        childIndexesByKey.set(key, [i]);
        computedMulti.push(constraintFromChild);
      } else {
        existing.push(i);
      }
    }

    if (computedMulti.length === 0) {
      return;
    }

    // Source returns parents in compareRows order within each chunk.
    // Merge across chunks to yield a globally ordered stream.
    const compareRows = this.#schema.compareRows;
    const compare: (a: Node, b: Node) => number = req.reverse
      ? (a, b) => compareRows(b.row, a.row)
      : (a, b) => compareRows(a.row, b.row);

    // Append our computed multi to whatever req.multiConstraints already
    // contained — chained FlippedJoins each contribute one entry, so the
    // source ANDs them all (e.g. `assigneeID IN (…) AND creatorID IN (…)`).
    const incoming = req.multiConstraints ?? [];
    const parentStream =
      computedMulti.length <= multiConstraintChunkSize
        ? this.#parent.fetch({
            ...req,
            multiConstraints: [...incoming, computedMulti],
          })
        : this.#fetchChunked(req, incoming, computedMulti, compare);

    for (const node of parentStream) {
      if (node === 'yield') {
        yield 'yield';
        continue;
      }
      const key = canonicalKey(node.row, parentKey);
      const idxs = childIndexesByKey.get(key);
      if (idxs === undefined) {
        // This row's parent-key doesn't match any of our computed
        // multi-constraint entries. Happens when our parent is an
        // intermediate operator (e.g. a chained FlippedJoin) that passes
        // multiConstraints through unchanged instead of filtering — see
        // FetchRequest.multiConstraints contract. The lookup miss here
        // performs the required filter, so just skip the row.
        continue;
      }
      // Children retain their original input order within the group
      // because we appended to `idxs` in iteration order.
      const relatedChildNodes: Node[] = idxs.map(i => childNodes[i]);
      yield* this.#yieldParentWithOverlay(node, relatedChildNodes);
    }
  }

  #fetchChunked(
    req: FetchRequest,
    incomingMultis: readonly MultiConstraint[],
    computedMulti: MultiConstraint,
    compare: (a: Node, b: Node) => number,
  ): Stream<Node | 'yield'> {
    const chunkStreams: Stream<Node | 'yield'>[] = [];
    for (let i = 0; i < computedMulti.length; i += multiConstraintChunkSize) {
      chunkStreams.push(
        this.#parent.fetch({
          ...req,
          multiConstraints: [
            ...incomingMultis,
            computedMulti.slice(i, i + multiConstraintChunkSize),
          ],
        }),
      );
    }
    return mergeSortedStreams(chunkStreams, compare);
  }

  *#yieldParentWithOverlay(
    minParentNode: Node,
    relatedChildNodes: Node[],
  ): Stream<Node> {
    let overlaidRelatedChildNodes = relatedChildNodes;

    let isParentInPushQueue: boolean;
    if (this.#boundProvider) {
      const partitionConstraint = this.#parentPartitionKey
        ? Object.fromEntries(
            this.#parentPartitionKey.map(k => [k, minParentNode.row[k]]),
          )
        : undefined;
      const bound = this.#boundProvider.getBound(partitionConstraint);
      isParentInPushQueue =
        bound !== undefined &&
        this.#inprogressChildChangePosition !== undefined &&
        this.#parent
          .getSchema()
          .compareRows(minParentNode.row, this.#inprogressChildChangePosition) >
          0 &&
        this.#parent.getSchema().compareRows(minParentNode.row, bound) <= 0;
    } else {
      isParentInPushQueue =
        this.#inprogressChildChangePosition !== undefined &&
        this.#parent
          .getSchema()
          .compareRows(minParentNode.row, this.#inprogressChildChangePosition) >
          0;
    }

    if (
      this.#inprogressChildChange &&
      this.#inprogressChildChangePosition &&
      isJoinMatch(
        this.#inprogressChildChange[ChangeIndex.NODE].row,
        this.#childKey,
        minParentNode.row,
        this.#parentKey,
      )
    ) {
      if (this.#inprogressChildChange[ChangeIndex.TYPE] === ChangeType.REMOVE) {
        if (!isParentInPushQueue) {
          // Remove from relatedChildNodes since the removed child
          // was inserted into childNodes above.
          overlaidRelatedChildNodes = relatedChildNodes.filter(
            n => n !== this.#inprogressChildChange?.[ChangeIndex.NODE],
          );
        }
      } else if (isParentInPushQueue) {
        overlaidRelatedChildNodes = [
          ...generateWithOverlayNoYield(
            relatedChildNodes,
            this.#inprogressChildChange,
            this.#child.getSchema(),
          ),
        ];
      }
    }

    // yield node if after the overlay it still has relationship nodes
    if (overlaidRelatedChildNodes.length > 0) {
      this.#indexParentRow(minParentNode.row);
      yield {
        ...minParentNode,
        relationships: {
          ...minParentNode.relationships,
          [this.#relationshipName]: () => overlaidRelatedChildNodes,
        },
      };
    }
  }

  *#pushChild(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
      case ChangeType.REMOVE:
        yield* this.#pushChildChange(change);
        break;
      case ChangeType.EDIT: {
        assert(
          rowEqualsForCompoundKey(
            change[ChangeIndex.OLD_NODE].row,
            change[ChangeIndex.NODE].row,
            this.#childKey,
          ),
          `Child edit must not change relationship.`,
        );
        yield* this.#pushChildChange(change, true);
        break;
      }
      case ChangeType.CHILD:
        yield* this.#pushChildChange(change, true);
        break;
    }
  }

  *#pushChildChange(change: Change, exists?: boolean): Stream<'yield'> {
    this.#inprogressChildChange = change;
    this.#inprogressChildChangePosition = undefined;
    try {
      const constraint = buildJoinConstraint(
        change[ChangeIndex.NODE].row,
        this.#childKey,
        this.#parentKey,
      );
      if (!constraint) {
        return;
      }
      const childRow = change[ChangeIndex.NODE].row;
      const changeType = change[ChangeIndex.TYPE];

      let matching: ReturnType<typeof getMatchingParentEntries> | undefined;
      if (this.#storage) {
        matching = getMatchingParentEntries(
          this.#storage,
          childRow,
          this.#childKey,
          this.#parentPartitionKey,
        );
        if (!matching) {
          // If no matching parent is resident in the view, REMOVE and EDIT
          // cannot affect any view-resident parent. (ADD and CHILD can qualify
          // previously absent parents).
          if (
            changeType !== ChangeType.ADD &&
            changeType !== ChangeType.CHILD
          ) {
            return;
          }
        }
      }

      let parentNodeStream: Stream<Node | 'yield'>;
      if (
        matching &&
        this.#parentPartitionKey &&
        changeType !== ChangeType.ADD &&
        changeType !== ChangeType.CHILD
      ) {
        if (matching.length === 1) {
          const [entry] = matching;
          parentNodeStream = this.#parent.fetch({
            constraint: entry.partitionConstraint
              ? {...constraint, ...entry.partitionConstraint}
              : constraint,
          });
        } else {
          const streams = matching.map(entry =>
            this.#parent.fetch({
              constraint: entry.partitionConstraint
                ? {...constraint, ...entry.partitionConstraint}
                : constraint,
            }),
          );
          const compare = (a: Node, b: Node) =>
            this.#schema.compareRows(a.row, b.row);
          parentNodeStream = mergeSortedStreams(streams, compare);
        }
      } else {
        parentNodeStream = this.#parent.fetch({constraint});
      }

      for (const parentNode of parentNodeStream) {
        if (parentNode === 'yield') {
          yield 'yield';
          continue;
        }
        this.#inprogressChildChange = change;
        this.#inprogressChildChangePosition = parentNode.row;
        const childNodeStream = () => {
          const constraint = buildJoinConstraint(
            parentNode.row,
            this.#parentKey,
            this.#childKey,
          );
          return constraint ? this.#child.fetch({constraint}) : [];
        };
        let parentExists = exists;
        if (!parentExists) {
          for (const childNode of childNodeStream()) {
            if (childNode === 'yield') {
              yield 'yield';
              continue;
            }
            if (
              this.#child
                .getSchema()
                .compareRows(childNode.row, change[ChangeIndex.NODE].row) !== 0
            ) {
              parentExists = true;
              break;
            }
          }
        }
        if (parentExists) {
          yield* this.#output.push(
            makeChildChange(
              {
                ...parentNode,
                relationships: {
                  ...parentNode.relationships,
                  [this.#relationshipName]: childNodeStream,
                },
              },
              {
                relationshipName: this.#relationshipName,
                change,
              },
            ),
            this,
          );
        } else {
          const newNode = {
            ...parentNode,
            relationships: {
              ...parentNode.relationships,
              [this.#relationshipName]: () => [change[ChangeIndex.NODE]],
            },
          };
          if (change[ChangeIndex.TYPE] === ChangeType.ADD) {
            this.#indexParentRow(parentNode.row);
            yield* this.#output.push(makeAddChange(newNode), this);
          } else {
            this.#unindexParentRow(parentNode.row);
            yield* this.#output.push(makeRemoveChange(newNode), this);
          }
        }
      }
    } finally {
      this.#inprogressChildChange = undefined;
      this.#inprogressChildChangePosition = undefined;
    }
  }

  #indexParentRow(row: Row): void {
    if (!this.#storage) {
      return;
    }
    indexParentInStorage(
      this.#storage,
      row,
      this.#parentKey,
      this.#parent.getSchema().primaryKey,
      this.#parentPartitionKey,
    );
  }

  #unindexParentRow(row: Row): void {
    if (!this.#storage) {
      return;
    }
    unindexParentInStorage(
      this.#storage,
      row,
      this.#parentKey,
      this.#parent.getSchema().primaryKey,
      this.#parentPartitionKey,
    );
  }

  *#pushParent(change: Change): Stream<'yield'> {
    const childNodeStream = (node: Node) => () => {
      const constraint = buildJoinConstraint(
        node.row,
        this.#parentKey,
        this.#childKey,
      );
      return constraint ? this.#child.fetch({constraint}) : [];
    };

    const flip = (node: Node) => ({
      ...node,
      relationships: {
        ...node.relationships,
        [this.#relationshipName]: childNodeStream(node),
      },
    });

    // If no related child don't push as this is an inner join.
    let hasRelatedChild = false;
    for (const node of childNodeStream(change[ChangeIndex.NODE])()) {
      if (node === 'yield') {
        yield 'yield';
        continue;
      } else {
        hasRelatedChild = true;
        break;
      }
    }
    if (!hasRelatedChild) {
      return;
    }

    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        this.#indexParentRow(change[ChangeIndex.NODE].row);
        yield* this.#output.push(
          makeAddChange(flip(change[ChangeIndex.NODE])),
          this,
        );
        break;
      case ChangeType.REMOVE:
        this.#unindexParentRow(change[ChangeIndex.NODE].row);
        yield* this.#output.push(
          makeRemoveChange(flip(change[ChangeIndex.NODE])),
          this,
        );
        break;
      case ChangeType.CHILD: {
        yield* this.#output.push(
          makeChildChange(
            flip(change[ChangeIndex.NODE]),
            change[ChangeIndex.CHILD_DATA],
          ),
          this,
        );
        break;
      }
      case ChangeType.EDIT: {
        assert(
          rowEqualsForCompoundKey(
            change[ChangeIndex.OLD_NODE].row,
            change[ChangeIndex.NODE].row,
            this.#parentKey,
          ),
          'Parent edit must not change relationship.',
        );
        this.#unindexParentRow(change[ChangeIndex.OLD_NODE].row);
        this.#indexParentRow(change[ChangeIndex.NODE].row);
        yield* this.#output.push(
          makeEditChange(
            flip(change[ChangeIndex.NODE]),
            flip(change[ChangeIndex.OLD_NODE]),
          ),
          this,
        );
        break;
      }
      default:
        unreachable(change);
    }
  }
}

export {canonicalKeyForTest};
