import {assert} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import type {Change} from './change.ts';
import {constraintMatchesRow, type Constraint} from './constraint.ts';
import {compareValues, valuesEqual, type Node} from './data.ts';
import {
  cleanupPartition,
  inputNeedsPartitionCleanup,
  type Input,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

/**
 * Implements `Input.cleanupPartition` for Join and FlippedJoin.
 *
 * The rows matching `constraint` are leaving the view of the pipeline above
 * the join. They actually leave the join's parent input only if the parent
 * chain maintains per-partition state for them (a partitioned Take/Cap).
 * Otherwise they remain present upstream: any child-pipeline state keyed off
 * them stays live, is maintained by pushes, and is cleaned up when the rows
 * are removed upstream.
 */
export function* cleanupJoinPartition(
  parent: Input,
  child: Input,
  parentKey: CompoundKey,
  childKey: CompoundKey,
  constraint: Constraint,
): Stream<'yield'> {
  if (!inputNeedsPartitionCleanup(parent)) {
    return;
  }
  if (child.cleanupPartition && inputNeedsPartitionCleanup(child)) {
    // Recursively clean up child partitions keyed off the departing rows.
    // The departing rows are enumerated (deduped by parent key value)
    // before the parent chain deletes its own partition state below, while
    // they can still be fetched.
    const departing = new Map<string, Row>();
    for (const node of parent.fetch({constraint})) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      const key = JSON.stringify(parentKey.map(k => node.row[k] ?? null));
      if (!departing.has(key)) {
        departing.set(key, node.row);
      }
    }
    for (const row of departing.values()) {
      yield* cleanupChildJoinPartitionForRow(
        parent,
        child,
        parentKey,
        childKey,
        row,
        constraint,
      );
    }
  }
  yield* cleanupPartition(parent, constraint);
}

/**
 * Deletes the child pipeline's partition state keyed off `row`'s parent key,
 * unless some other row with the same parent key value is still present in
 * the join's parent input (in which case the partition is still reachable
 * and its state must be kept).
 *
 * Called after `row` was removed from the parent input, or, during
 * {@link cleanupJoinPartition}, for each departing row. In the latter case
 * `excludeConstraint` is the constraint identifying the departing rows,
 * which have not been cleaned out of the parent chain yet and must be
 * ignored when checking whether the parent key value is still present.
 */
export function* cleanupChildJoinPartitionForRow(
  parent: Input,
  child: Input,
  parentKey: CompoundKey,
  childKey: CompoundKey,
  row: Row,
  excludeConstraint: Constraint | undefined,
): Stream<'yield'> {
  if (!child.cleanupPartition || !inputNeedsPartitionCleanup(child)) {
    return;
  }
  const parentConstraint = buildJoinConstraint(row, parentKey, parentKey);
  const childConstraint = buildJoinConstraint(row, parentKey, childKey);
  if (!parentConstraint || !childConstraint) {
    // Null join key values: the child pipeline is never fetched for them,
    // so there is no partition state to clean up.
    return;
  }
  for (const node of parent.fetch({constraint: parentConstraint})) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    if (
      excludeConstraint &&
      constraintMatchesRow(excludeConstraint, node.row)
    ) {
      continue;
    }
    // Another parent row still maps to this child partition.
    return;
  }
  yield* child.cleanupPartition(childConstraint);
}

export function generateWithOverlayNoYield(
  stream: Stream<Node>,
  overlay: Change,
  schema: SourceSchema,
): Stream<Node> {
  return generateWithOverlay(stream, overlay, schema) as Stream<Node>;
}

export function* generateWithOverlay(
  stream: Stream<Node | 'yield'>,
  overlay: Change,
  schema: SourceSchema,
): Stream<Node | 'yield'> {
  let applied = false;
  let editOldApplied = false;
  let editNewApplied = false;
  for (const node of stream) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    let yieldNode = true;
    if (!applied) {
      switch (overlay[ChangeIndex.TYPE]) {
        case ChangeType.ADD: {
          if (
            schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) === 0
          ) {
            applied = true;
            yieldNode = false;
          }
          break;
        }
        case ChangeType.REMOVE: {
          if (schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) < 0) {
            applied = true;
            yield overlay[ChangeIndex.NODE];
          }
          break;
        }
        case ChangeType.EDIT: {
          if (
            !editOldApplied &&
            schema.compareRows(overlay[ChangeIndex.OLD_NODE].row, node.row) < 0
          ) {
            editOldApplied = true;
            if (editNewApplied) {
              applied = true;
            }
            yield overlay[ChangeIndex.OLD_NODE];
          }
          if (
            !editNewApplied &&
            schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) === 0
          ) {
            editNewApplied = true;
            if (editOldApplied) {
              applied = true;
            }
            yieldNode = false;
          }
          break;
        }
        case ChangeType.CHILD: {
          if (
            schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) === 0
          ) {
            applied = true;
            yield {
              row: node.row,
              relationships: {
                ...node.relationships,
                [overlay[ChangeIndex.CHILD_DATA].relationshipName]: () =>
                  generateWithOverlay(
                    node.relationships[
                      overlay[ChangeIndex.CHILD_DATA].relationshipName
                    ](),
                    overlay[ChangeIndex.CHILD_DATA].change,
                    schema.relationships[
                      overlay[ChangeIndex.CHILD_DATA].relationshipName
                    ],
                  ),
              },
            };
            yieldNode = false;
          }
          break;
        }
      }
    }
    if (yieldNode) {
      yield node;
    }
  }
  if (!applied) {
    if (overlay[ChangeIndex.TYPE] === ChangeType.REMOVE) {
      applied = true;
      yield overlay[ChangeIndex.NODE];
    } else if (overlay[ChangeIndex.TYPE] === ChangeType.EDIT) {
      assert(
        editNewApplied,
        'edit overlay: new node must be applied before old node',
      );
      editOldApplied = true;
      applied = true;
      yield overlay[ChangeIndex.OLD_NODE];
    }
  }

  assert(
    applied,
    'overlayGenerator: overlay was never applied to any fetched node',
  );
}

export function generateWithOverlayNoYieldUnordered(
  stream: Stream<Node>,
  overlay: Change,
  schema: SourceSchema,
): Stream<Node> {
  return generateWithOverlayUnordered(stream, overlay, schema) as Stream<Node>;
}

export function* generateWithOverlayUnordered(
  stream: Stream<Node | 'yield'>,
  overlay: Change,
  schema: SourceSchema,
): Stream<Node | 'yield'> {
  // Eager inject
  if (overlay[ChangeIndex.TYPE] === ChangeType.REMOVE) {
    yield overlay[ChangeIndex.NODE];
  } else if (overlay[ChangeIndex.TYPE] === ChangeType.EDIT) {
    yield overlay[ChangeIndex.OLD_NODE];
  }

  // Stream with inline suppress
  let suppressed = false;
  for (const node of stream) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    if (!suppressed) {
      if (
        overlay[ChangeIndex.TYPE] === ChangeType.ADD ||
        overlay[ChangeIndex.TYPE] === ChangeType.EDIT
      ) {
        if (
          rowEqualsForCompoundKey(
            overlay[ChangeIndex.NODE].row,
            node.row,
            schema.primaryKey,
          )
        ) {
          suppressed = true;
          continue;
        }
      }
      if (overlay[ChangeIndex.TYPE] === ChangeType.CHILD) {
        if (
          rowEqualsForCompoundKey(
            overlay[ChangeIndex.NODE].row,
            node.row,
            schema.primaryKey,
          )
        ) {
          suppressed = true;
          yield {
            row: node.row,
            relationships: {
              ...node.relationships,
              [overlay[ChangeIndex.CHILD_DATA].relationshipName]: () =>
                generateWithOverlay(
                  node.relationships[
                    overlay[ChangeIndex.CHILD_DATA].relationshipName
                  ](),
                  overlay[ChangeIndex.CHILD_DATA].change,
                  schema.relationships[
                    overlay[ChangeIndex.CHILD_DATA].relationshipName
                  ],
                ),
            },
          };
          continue;
        }
      }
    }
    yield node;
  }
  assert(
    suppressed || overlay[ChangeIndex.TYPE] === ChangeType.REMOVE,
    'overlayGenerator: overlay was never applied to any fetched node',
  );
}

export function rowEqualsForCompoundKey(
  a: Row,
  b: Row,
  key: CompoundKey,
): boolean {
  for (let i = 0; i < key.length; i++) {
    if (compareValues(a[key[i]], b[key[i]]) !== 0) {
      return false;
    }
  }
  return true;
}

export function isJoinMatch(
  parent: Row,
  parentKey: CompoundKey,
  child: Row,
  childKey: CompoundKey,
) {
  for (let i = 0; i < parentKey.length; i++) {
    if (!valuesEqual(parent[parentKey[i]], child[childKey[i]])) {
      return false;
    }
  }
  return true;
}

/**
 * Builds a constraint object by mapping values from `sourceRow` using `sourceKey`
 * to keys specified by `targetKey`. Returns `undefined` if any source value is `null`,
 * since null foreign keys cannot match any rows.
 */
export function buildJoinConstraint(
  sourceRow: Row,
  sourceKey: CompoundKey,
  targetKey: CompoundKey,
): Record<string, Value> | undefined {
  const constraint: Record<string, Value> = {};
  for (let i = 0; i < targetKey.length; i++) {
    const value = sourceRow[sourceKey[i]];
    if (value === null) {
      return undefined;
    }
    constraint[targetKey[i]] = value;
  }
  return constraint;
}
