import {
  assert,
  assertArray,
  assertObject,
  unreachable,
} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {drainStreams, type Comparator, type Node} from './data.ts';
import type {SourceSchema} from './schema.ts';
import type {Entry, EntryList, Format} from './view.ts';

/**
 * `applyChange` does not consume the `relationships` of `ChildChange#node`,
 * `EditChange#node` and `EditChange#oldNode`.  The `ViewChange` type
 * documents and enforces this via the type system.
 */
export type ViewChange =
  | AddViewChange
  | RemoveViewChange
  | ChildViewChange
  | EditViewChange;

export type RowOnlyNode = {row: Row};

export type AddViewChange = {
  type: 'add';
  node: Node;
};

export type RemoveViewChange = {
  type: 'remove';
  node: Node;
};

type ChildViewChange = {
  type: 'child';
  node: RowOnlyNode;
  child: {
    relationshipName: string;
    change: ViewChange;
  };
};

type EditViewChange = {
  type: 'edit';
  node: RowOnlyNode;
  oldNode: RowOnlyNode;
};

/**
 * This is a subset of WeakMap but restricted to what we need.
 */
export interface RefCountMap {
  get(entry: Entry): number | undefined;
  set(entry: Entry, refCount: number): void;
  delete(entry: Entry): boolean;
}

export function applyChange(
  parentEntry: Entry,
  change: ViewChange,
  schema: SourceSchema,
  relationship: string,
  format: Format,
  refCountMap: RefCountMap,
) {
  if (schema.isHidden) {
    switch (change.type) {
      case 'add':
      case 'remove':
        for (const [relationship, children] of Object.entries(
          change.node.relationships,
        )) {
          const childSchema = must(schema.relationships[relationship]);
          for (const node of children()) {
            applyChange(
              parentEntry,
              {type: change.type, node},
              childSchema,
              relationship,
              format,
              refCountMap,
            );
          }
        }
        return;
      case 'edit':
        // If hidden at this level it means that the hidden row was changed. If
        // the row was changed in such a way that it would change the
        // relationships then the edit would have been split into remove and
        // add.
        return;
      case 'child': {
        const childSchema = must(
          schema.relationships[change.child.relationshipName],
        );
        applyChange(
          parentEntry,
          change.child.change,
          childSchema,
          relationship,
          format,
          refCountMap,
        );
        return;
      }
      default:
        unreachable(change);
    }
  }

  const {singular, relationships: childFormats} = format;
  switch (change.type) {
    case 'add': {
      // TODO: Only create a new entry if we need to mutate the existing one.
      const newEntry: Entry = {
        ...change.node.row,
      };
      if (singular) {
        const oldEntry = parentEntry[relationship] as Entry | undefined;
        if (oldEntry !== undefined) {
          assert(
            schema.compareRows(oldEntry, newEntry) === 0,
            'single output already exists',
          );
          // adding same again.
          const rc = must(refCountMap.get(oldEntry));
          refCountMap.delete(oldEntry);
          // @ts-expect-error parentEntry is readonly
          parentEntry[relationship] = newEntry;
          refCountMap.set(newEntry, rc + 1);
        } else {
          // @ts-expect-error parentEntry is readonly
          parentEntry[relationship] = newEntry;
          refCountMap.set(newEntry, 1);
        }
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        const {pos, found} = binarySearch(view, newEntry, schema.compareRows);

        let deleteCount = 0;
        let rc = 1;
        if (found) {
          deleteCount = 1;
          rc = must(refCountMap.get(view[pos])) + 1;
          refCountMap.delete(view[pos]);
        }

        // @ts-expect-error view is readonly
        view.splice(pos, deleteCount, newEntry);
        refCountMap.set(newEntry, rc);
      }
      for (const [relationship, children] of Object.entries(
        change.node.relationships,
      )) {
        // TODO: Is there a flag to make TypeScript complain that dictionary access might be undefined?
        const childSchema = must(schema.relationships[relationship]);
        const childFormat = childFormats[relationship];
        if (childFormat === undefined) {
          continue;
        }

        const newView = childFormat.singular ? undefined : ([] as EntryList);
        // @ts-expect-error newEntry is readonly
        newEntry[relationship] = newView;
        for (const node of children()) {
          applyChange(
            newEntry,
            {type: 'add', node},
            childSchema,
            relationship,
            childFormat,
            refCountMap,
          );
        }
      }
      break;
    }
    case 'remove': {
      if (singular) {
        const oldEntry = parentEntry[relationship] as Entry | undefined;
        assert(oldEntry !== undefined, 'node does not exist');
        const rc = must(refCountMap.get(oldEntry));
        if (rc === 1) {
          refCountMap.delete(oldEntry);
          // @ts-expect-error parentEntry is readonly
          parentEntry[relationship] = undefined;
        } else {
          refCountMap.set(oldEntry, rc - 1);
        }
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        const {pos, found} = binarySearch(
          view,
          change.node.row,
          schema.compareRows,
        );
        assert(found, 'node does not exist');
        const rc = must(refCountMap.get(view[pos]));
        if (rc === 1) {
          refCountMap.delete(view[pos]);
          // @ts-expect-error view is readonly
          view.splice(pos, 1);
        } else {
          refCountMap.set(view[pos], rc - 1);
        }
      }
      // Needed to ensure cleanup of operator state is fully done.
      drainStreams(change.node);
      break;
    }
    case 'child': {
      let existing: Entry;
      if (singular) {
        assertObject(parentEntry[relationship]);
        existing = parentEntry[relationship];
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        const {pos, found} = binarySearch(
          view,
          change.node.row,
          schema.compareRows,
        );
        assert(found, 'node does not exist');
        existing = view[pos];
      }

      const childSchema = must(
        schema.relationships[change.child.relationshipName],
      );
      const childFormat = format.relationships[change.child.relationshipName];
      if (childFormat !== undefined) {
        applyChange(
          existing,
          change.child.change,
          childSchema,
          change.child.relationshipName,
          childFormat,
          refCountMap,
        );
      }
      break;
    }
    case 'edit': {
      if (singular) {
        assertObject(parentEntry[relationship]);
        const existing = parentEntry[relationship];
        const rc = must(refCountMap.get(existing));
        const newEntry = {
          ...parentEntry[relationship],
          ...change.node.row,
        };
        refCountMap.set(newEntry, rc);
        refCountMap.delete(existing);
        // @ts-expect-error parentEntry is readonly
        parentEntry[relationship] = newEntry;
      } else {
        const view = parentEntry[relationship] as EntryList | undefined;
        assertArray(view);
        // If the order changed due to the edit, we need to remove and reinsert.
        if (schema.compareRows(change.oldNode.row, change.node.row) === 0) {
          const {pos, found} = binarySearch(
            view,
            change.oldNode.row,
            schema.compareRows,
          );
          assert(found, 'node does not exists');
          const rc = must(refCountMap.get(view[pos]));
          refCountMap.delete(view[pos]);
          view[pos] = makeEntryPreserveRelationships(
            change.node.row,
            view[pos],
            format.relationships,
          );
          refCountMap.set(view[pos], rc);
        } else {
          // Remove
          const {pos, found} = binarySearch(
            view,
            change.oldNode.row,
            schema.compareRows,
          );
          assert(found, 'node does not exists');
          const oldEntry = view[pos];
          const rc = must(refCountMap.get(oldEntry));
          if (rc === 1) {
            refCountMap.delete(oldEntry);
            view.splice(pos, 1);
          } else {
            refCountMap.set(oldEntry, rc - 1);
          }

          // Insert
          {
            const {pos, found} = binarySearch(
              view,
              change.node.row,
              schema.compareRows,
            );
            let rc = 1;
            let newEntry: Entry;
            let deleteCount = 0;
            if (found) {
              // We changed a row to a row that already exists.
              // The existing row should increase its ref count.
              // We preserver the relationships of the existing row.
              deleteCount = 1;
              const oldEntry = view[pos];
              rc = must(refCountMap.get(oldEntry)) + 1;
              refCountMap.delete(oldEntry);
              newEntry = makeEntryPreserveRelationships(
                change.node.row,
                oldEntry,
                format.relationships,
              );
            } else {
              newEntry = makeEntryPreserveRelationships(
                change.node.row,
                oldEntry,
                format.relationships,
              );
            }
            view.splice(pos, deleteCount, newEntry);
            refCountMap.set(newEntry, rc);
          }
        }
      }
      break;
    }
    default:
      unreachable(change);
  }
}

// TODO: Do not return an object. It puts unnecessary pressure on the GC.
function binarySearch(view: EntryList, target: Entry, comparator: Comparator) {
  let low = 0;
  let high = view.length - 1;
  while (low <= high) {
    const mid = (low + high) >>> 1;
    const comparison = comparator(view[mid] as Row, target as Row);
    if (comparison < 0) {
      low = mid + 1;
    } else if (comparison > 0) {
      high = mid - 1;
    } else {
      return {pos: mid, found: true};
    }
  }
  return {pos: low, found: false};
}

function makeEntryPreserveRelationships(
  row: Row,
  entry: Entry,
  relationships: {[key: string]: Format},
): Entry {
  const result: Entry = {...row};
  for (const relationship in relationships) {
    assert(!(relationship in row), 'Relationship already exists');
    // @ts-expect-error entry is readonly
    result[relationship] = entry[relationship];
  }
  return result;
}

function getChildEntryList(
  parentEntry: Entry,
  relationship: string,
): EntryList {
  const view = parentEntry[relationship] as unknown;
  assertArray(view);
  return view as EntryList;
}
