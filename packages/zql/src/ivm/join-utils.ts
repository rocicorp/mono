import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';
import {compareValues, valuesEqual, type Node} from './data.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';

export type JoinChangeOverlay = {
  change: Change;
  position: Row | undefined;
};

export function* generateWithOverlay(
  stream: Stream<Node>,
  overlay: Change,
  schema: SourceSchema,
): Stream<Node> {
  let applied = false;
  let editOldApplied = false;
  let editNewApplied = false;
  for (const node of stream) {
    let yieldNode = true;
    if (!applied) {
      switch (overlay.type) {
        case 'add': {
          if (schema.compareRows(overlay.node.row, node.row) === 0) {
            applied = true;
            yieldNode = false;
          }
          break;
        }
        case 'remove': {
          if (schema.compareRows(overlay.node.row, node.row) < 0) {
            applied = true;
            yield overlay.node;
          }
          break;
        }
        case 'edit': {
          if (
            !editOldApplied &&
            schema.compareRows(overlay.oldNode.row, node.row) < 0
          ) {
            editOldApplied = true;
            if (editNewApplied) {
              applied = true;
            }
            yield overlay.oldNode;
          }
          if (
            !editNewApplied &&
            schema.compareRows(overlay.node.row, node.row) === 0
          ) {
            editNewApplied = true;
            if (editOldApplied) {
              applied = true;
            }
            yieldNode = false;
          }
          break;
        }
        case 'child': {
          if (schema.compareRows(overlay.node.row, node.row) === 0) {
            applied = true;
            yield {
              row: node.row,
              relationships: {
                ...node.relationships,
                [overlay.child.relationshipName]: () =>
                  generateWithOverlay(
                    node.relationships[overlay.child.relationshipName](),
                    overlay.child.change,
                    schema.relationships[overlay.child.relationshipName],
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
    if (overlay.type === 'remove') {
      applied = true;
      yield overlay.node;
    } else if (overlay.type === 'edit') {
      assert(editNewApplied);
      editOldApplied = true;
      applied = true;
      yield overlay.oldNode;
    }
  }

  assert(applied);
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
