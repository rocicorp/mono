import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey, System} from '../../../zero-protocol/src/ast.ts';
import type {Value} from '../../../zero-protocol/src/data.ts';
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
  JoinIndex,
  rowEqualsForCompoundKey,
  type JoinStorage,
} from './join-utils.ts';
import {mergeSortedStreams} from './memory-source.ts';
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
  storage: Storage;
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
  readonly #boundProvider: TakeBoundProvider | undefined;
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
    boundProvider,
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
    this.#boundProvider = boundProvider;
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
      yield* this.#yieldParent(node, relatedChildNodes);
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

  *#yieldParent(minParentNode: Node, relatedChildNodes: Node[]): Stream<Node> {
    if (relatedChildNodes.length > 0) {
      this.#joinIndex.index(minParentNode.row, relatedChildNodes.length);
      yield {
        ...minParentNode,
        relationships: {
          ...minParentNode.relationships,
          [this.#relationshipName]: () => relatedChildNodes,
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

    const matching = this.#joinIndex.getMatching(childRow, this.#childKey);
    if (!matching) {
      // If no matching parent is resident in the view, REMOVE and EDIT
      // cannot affect any view-resident parent. (ADD and CHILD can qualify
      // previously absent parents).
      if (changeType !== ChangeType.ADD && changeType !== ChangeType.CHILD) {
        return;
      }
    }

    let fetchConstraints: Constraint[];
    if (changeType !== ChangeType.ADD && changeType !== ChangeType.CHILD) {
      assert(matching, 'Matching entries must exist for non-add child change');
      fetchConstraints = matching.map(entry =>
        entry.partitionConstraint
          ? {...constraint, ...entry.partitionConstraint}
          : constraint,
      );
    } else {
      fetchConstraints = [constraint];
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

    const visitedPks = new Set<string>();

    for (const parentNode of parentNodeStream) {
      if (parentNode === 'yield') {
        yield 'yield';
        continue;
      }
      const childNodeStream = () => {
        const constraint = buildJoinConstraint(
          parentNode.row,
          this.#parentKey,
          this.#childKey,
        );
        return constraint ? this.#child.fetch({constraint}) : [];
      };
      const parentPk = canonicalKey(
        parentNode.row,
        this.#parent.getSchema().primaryKey,
      );
      visitedPks.add(parentPk);
      let parentInStorage = this.#joinIndex.has(parentNode.row);
      if (parentInStorage && this.#boundProvider) {
        const bound = this.#boundProvider.getBound();
        if (
          bound !== undefined &&
          this.#schema.compareRows(parentNode.row, bound) > 0
        ) {
          this.#joinIndex.unindex(parentNode.row);
          parentInStorage = false;
        }
      }

      if (changeType === ChangeType.REMOVE && !parentInStorage) {
        continue;
      }

      let parentExists = exists;
      if (parentExists === undefined) {
        if (changeType === ChangeType.ADD) {
          const {oldCount} = this.#joinIndex.increment(parentNode.row);
          parentExists = oldCount > 0;
        } else if (changeType === ChangeType.REMOVE) {
          const {oldCount, newCount} = this.#joinIndex.decrement(
            parentNode.row,
          );
          if (oldCount === undefined) {
            continue;
          }
          parentExists = newCount > 0;
        } else {
          parentExists = true;
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
          yield* this.#output.push(makeAddChange(newNode), this);
        } else {
          yield* this.#output.push(makeRemoveChange(newNode), this);
        }
      }
    }

    if (matching) {
      const joinKey = canonicalKey(childRow, this.#childKey);
      for (const entry of matching) {
        for (const pk of entry.pks) {
          if (!visitedPks.has(pk)) {
            this.#joinIndex.delEntry(joinKey, pk, entry.partitionConstraint);
          }
        }
      }
    }
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
    let childCount = 0;
    for (const node of childNodeStream(change[ChangeIndex.NODE])()) {
      if (node === 'yield') {
        yield 'yield';
        continue;
      } else {
        childCount++;
      }
    }
    if (childCount === 0) {
      return;
    }

    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        this.#joinIndex.index(change[ChangeIndex.NODE].row, childCount);
        yield* this.#output.push(
          makeAddChange(flip(change[ChangeIndex.NODE])),
          this,
        );
        break;
      case ChangeType.REMOVE:
        this.#joinIndex.unindex(change[ChangeIndex.NODE].row);
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
        const count =
          this.#joinIndex.get(change[ChangeIndex.OLD_NODE].row) ?? childCount;
        this.#joinIndex.unindex(change[ChangeIndex.OLD_NODE].row);
        this.#joinIndex.index(change[ChangeIndex.NODE].row, count);
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
