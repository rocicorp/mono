import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {binarySearch} from '../../../shared/src/binary-search.ts';
import {must} from '../../../shared/src/must.ts';
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
  generateWithOverlayNoYield,
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

type Args = {
  parent: Input;
  child: Input;
  // The nth key in childKey corresponds to the nth key in parentKey.
  parentKey: CompoundKey;
  childKey: CompoundKey;

  relationshipName: string;
  hidden: boolean;
  system: System;
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

    yield* this.#fetchMergeSort(req, childNodes);
  }

  *#fetchMergeSort(
    req: FetchRequest,
    childNodes: Node[],
  ): Stream<Node | 'yield'> {
    // Group children by parent-key value so children sharing a value
    // share one fetch (and one cursor). Without this, two children with
    // the same parent-key value would each open their own iterator that
    // re-fetches the same parent rows — wasted IO.
    const parentKey = this.#parentKey;
    const computedKeys: Constraint[] = [];
    const childIndexesByKey = new Map<string, number[]>();
    for (let i = 0; i < childNodes.length; i++) {
      const constraintFromChild = buildJoinConstraint(
        childNodes[i].row,
        this.#childKey,
        parentKey,
      );
      if (
        !constraintFromChild ||
        (req.constraint &&
          !constraintsAreCompatible(constraintFromChild, req.constraint))
      ) {
        continue;
      }
      const key = canonicalKey(constraintFromChild, parentKey);
      const existing = childIndexesByKey.get(key);
      if (existing === undefined) {
        childIndexesByKey.set(key, [i]);
        computedKeys.push(constraintFromChild);
      } else {
        existing.push(i);
      }
    }

    if (computedKeys.length === 0) {
      return;
    }

    const compareRows = this.#schema.compareRows;
    const compare: (a: Node, b: Node) => number = req.reverse
      ? (a, b) => compareRows(b.row, a.row)
      : (a, b) => compareRows(a.row, b.row);

    // One stream per unique parent-key value. Each stream returns its
    // matching parent rows in compareRows order; the heap merges them
    // into a globally ordered stream. Distinct rows can't compare equal
    // (compareRows includes the primary key), so no tie handling needed
    // — every emit maps back to exactly one entry in childIndexesByKey.
    const streams: Stream<Node | 'yield'>[] = computedKeys.map(c =>
      this.#parent.fetch({
        ...req,
        constraint: req.constraint ? {...req.constraint, ...c} : c,
      }),
    );

    for (const node of mergeSortedStreams(streams, compare)) {
      if (node === 'yield') {
        yield 'yield';
        continue;
      }
      // Every fetched parent row matches the constraint for one entry
      // in `computedKeys`, whose canonical key was inserted into the
      // map — so the lookup is guaranteed to hit. Children retain
      // their original input order within the group because we
      // appended to the indexes array in iteration order.
      const idxs = must(
        childIndexesByKey.get(canonicalKey(node.row, parentKey)),
      );
      const relatedChildNodes: Node[] = idxs.map(i => childNodes[i]);
      yield* this.#yieldParentWithOverlay(node, relatedChildNodes);
    }
  }

  *#yieldParentWithOverlay(
    minParentNode: Node,
    relatedChildNodes: Node[],
  ): Stream<Node> {
    let overlaidRelatedChildNodes = relatedChildNodes;
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
      const hasInprogressChildChangeBeenPushedForMinParentNode =
        this.#parent
          .getSchema()
          .compareRows(
            minParentNode.row,
            this.#inprogressChildChangePosition,
          ) <= 0;
      if (this.#inprogressChildChange[ChangeIndex.TYPE] === ChangeType.REMOVE) {
        if (hasInprogressChildChangeBeenPushedForMinParentNode) {
          // Remove from relatedChildNodes since the removed child
          // was inserted into childNodes above.
          overlaidRelatedChildNodes = relatedChildNodes.filter(
            n => n !== this.#inprogressChildChange?.[ChangeIndex.NODE],
          );
        }
      } else if (!hasInprogressChildChangeBeenPushedForMinParentNode) {
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
      const parentNodeStream = constraint
        ? this.#parent.fetch({constraint})
        : [];
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
        if (!exists) {
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
              exists = true;
              break;
            }
          }
        }
        if (exists) {
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
          yield* this.#output.push(
            change[ChangeIndex.TYPE] === ChangeType.ADD
              ? makeAddChange(newNode)
              : makeRemoveChange(newNode),
            this,
          );
        }
      }
    } finally {
      this.#inprogressChildChange = undefined;
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
        yield* this.#output.push(
          makeAddChange(flip(change[ChangeIndex.NODE])),
          this,
        );
        break;
      case ChangeType.REMOVE:
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
          `Parent edit must not change relationship.`,
        );
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

/**
 * Canonical string key over `keys` of `record`, used by `#fetchMergeSort`
 * both to dedupe per-child fetches and to map each returned parent row
 * back to the children that referenced its parent-key tuple.
 *
 * Exported for testing.
 */
export function canonicalKey(
  record: Record<string, Value | undefined>,
  keys: CompoundKey,
): string {
  if (keys.length === 1) {
    return canonicalValue(record[keys[0]]);
  }
  let s = '';
  for (let i = 0; i < keys.length; i++) {
    if (i > 0) s += '\x00';
    s += canonicalValue(record[keys[i]]);
  }
  return s;
}

function canonicalValue(v: Value | bigint | undefined): string {
  // Tag by type so we don't conflate e.g. `1` (number) with `"1"` (string).
  // Bigint shows up at runtime when zqlite's safeIntegers is on, even
  // though the static `Value` type doesn't list it.
  if (v === null || v === undefined) return 'n';
  const t = typeof v;
  if (t === 'string') return 's' + (v as string);
  if (t === 'number') return 'd' + (v as number);
  if (t === 'bigint') return 'b' + (v as bigint).toString();
  if (t === 'boolean') return v ? 't' : 'f';
  return 'j' + JSON.stringify(v);
}
