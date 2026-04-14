import {assert} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import type {Change} from './change.ts';
import {compareValues, valuesEqual, type Node} from './data.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

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
