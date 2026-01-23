import {
  assert,
  assertArray,
  assertNumber,
  unreachable,
} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {type Comparator, type Node} from './data.ts';
import {skipYields} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Entry, Format} from './view.ts';

export const refCountSymbol = Symbol('rc');
export const idSymbol = Symbol('id');

type MetaEntry = Writable<Entry> & {
  [refCountSymbol]: number;
  [idSymbol]?: string | undefined;
};
type MetaEntryList = readonly MetaEntry[];

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
 * @deprecated Not used anymore. This will be removed in the future.
 */
export interface RefCountMap {
  get(entry: Entry): number | undefined;
  set(entry: Entry, refCount: number): void;
  delete(entry: Entry): boolean;
}

/**
 * Applies a change to the view immutably. Returns a new Entry if any changes
 * were made, or the same Entry if nothing changed. Unchanged row objects keep
 * their identity, enabling React.memo with shallow comparison to work effectively.
 */
export function applyChange(
  parentEntry: Entry,
  change: ViewChange,
  schema: SourceSchema,
  relationship: string,
  format: Format,
  withIDs = false,
): Entry {
  if (schema.isHidden) {
    switch (change.type) {
      case 'add':
      case 'remove': {
        let currentParent = parentEntry;
        for (const [relationship, children] of Object.entries(
          change.node.relationships,
        )) {
          const childSchema = must(schema.relationships[relationship]);
          for (const node of skipYields(children())) {
            currentParent = applyChange(
              currentParent,
              {type: change.type, node},
              childSchema,
              relationship,
              format,
              withIDs,
            );
          }
        }
        return currentParent;
      }
      case 'edit':
        // If hidden at this level it means that the hidden row was changed. If
        // the row was changed in such a way that it would change the
        // relationships then the edit would have been split into remove and
        // add.
        return parentEntry;
      case 'child': {
        const childSchema = must(
          schema.relationships[change.child.relationshipName],
        );
        return applyChange(
          parentEntry,
          change.child.change,
          childSchema,
          relationship,
          format,
          withIDs,
        );
      }
      default:
        unreachable(change);
    }
  }

  const {singular, relationships: childFormats} = format;
  switch (change.type) {
    case 'add': {
      if (singular) {
        const oldEntry = parentEntry[relationship] as MetaEntry | undefined;
        if (oldEntry !== undefined) {
          assert(
            schema.compareRows(oldEntry, change.node.row) === 0,
            `Singular relationship '${relationship}' should not have multiple rows. You may need to declare this relationship with the \`many\` helper instead of the \`one\` helper in your schema.`,
          );
          // adding same again: create new entry with incremented refCount
          const newEntry: MetaEntry = {
            ...oldEntry,
            [refCountSymbol]: oldEntry[refCountSymbol] + 1,
          };
          return {...parentEntry, [relationship]: newEntry};
        } else {
          let newEntry = makeNewMetaEntry(change.node.row, schema, withIDs, 1);
          newEntry = initializeRelationships(
            newEntry,
            change.node,
            schema,
            childFormats,
            withIDs,
          );
          return {...parentEntry, [relationship]: newEntry};
        }
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        const {newEntry, newView} = add(
          change.node.row,
          view,
          schema,
          withIDs,
        );

        if (newEntry) {
          const initializedEntry = initializeRelationships(
            newEntry,
            change.node,
            schema,
            childFormats,
            withIDs,
          );
          // Replace the entry in the view if relationships were added
          if (initializedEntry !== newEntry) {
            const idx = newView.indexOf(newEntry);
            return {
              ...parentEntry,
              [relationship]: newView.with(idx, initializedEntry),
            };
          }
        }
        return {...parentEntry, [relationship]: newView};
      }
    }
    case 'remove': {
      if (singular) {
        const oldEntry = parentEntry[relationship] as MetaEntry | undefined;
        assert(oldEntry !== undefined, 'node does not exist');
        const rc = oldEntry[refCountSymbol];
        if (rc === 1) {
          return {...parentEntry, [relationship]: undefined};
        }
        const newEntry: MetaEntry = {
          ...oldEntry,
          [refCountSymbol]: rc - 1,
        };
        return {...parentEntry, [relationship]: newEntry};
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        const newView = removeAndUpdateRefCount(
          view,
          change.node.row,
          schema.compareRows,
        );
        return {...parentEntry, [relationship]: newView};
      }
    }
    case 'child': {
      const childSchema = must(
        schema.relationships[change.child.relationshipName],
      );
      const childFormat = format.relationships[change.child.relationshipName];
      if (childFormat === undefined) {
        return parentEntry;
      }

      if (singular) {
        const existing = getSingularEntry(parentEntry, relationship);
        const newExisting = applyChange(
          existing,
          change.child.change,
          childSchema,
          change.child.relationshipName,
          childFormat,
          withIDs,
        );
        if (newExisting === existing) {
          return parentEntry;
        }
        return {...parentEntry, [relationship]: newExisting};
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        const {pos, found} = binarySearch(
          view,
          change.node.row,
          schema.compareRows,
        );
        assert(found, 'node does not exist');
        const existing = view[pos];
        const newExisting = applyChange(
          existing,
          change.child.change,
          childSchema,
          change.child.relationshipName,
          childFormat,
          withIDs,
        );
        if (newExisting === existing) {
          return parentEntry;
        }
        return {
          ...parentEntry,
          [relationship]: view.with(pos, newExisting as MetaEntry),
        };
      }
    }
    case 'edit': {
      if (singular) {
        const existing = parentEntry[relationship];
        assertMetaEntry(existing);
        const newEntry = applyEdit(existing, change, schema, withIDs);
        return {...parentEntry, [relationship]: newEntry};
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        // The position of the row in the list may have changed due to the edit.
        if (schema.compareRows(change.oldNode.row, change.node.row) !== 0) {
          const {pos: oldPos, found: oldFound} = binarySearch(
            view,
            change.oldNode.row,
            schema.compareRows,
          );
          assert(oldFound, 'old node does not exist');
          const oldEntry = view[oldPos];
          const {pos, found} = binarySearch(
            view,
            change.node.row,
            schema.compareRows,
          );
          // A special case:
          // when refCount is 1 (so the row is being moved
          // without leaving a placeholder behind), and the new pos is
          // the same as the old, or directly after the old (so after the remove
          // of the old it would be in the same pos):
          // the row does not need to be moved, it can just be edited in place.
          if (
            oldEntry[refCountSymbol] === 1 &&
            (pos === oldPos || pos - 1 === oldPos)
          ) {
            const newEntry = applyEdit(oldEntry, change, schema, withIDs);
            return {
              ...parentEntry,
              [relationship]: view.with(oldPos, newEntry),
            };
          } else {
            // Move the row.  If the row has > 1 ref count, an edit should
            // be received for each ref count.  On the first edit, the original
            // row is moved, the edit is applied to it and its ref count is set
            // to 1.  A shallow copy of the row is left at the old pos for
            // processing of the remaining edit, and the copy's ref count
            // is decremented.  As each edit is received the ref count of the
            // copy is decrement, and the ref count of the row at the new
            // position is incremented.  When the copy's ref count goes to 0,
            // it is removed.
            const newRefCount = oldEntry[refCountSymbol] - 1;
            let newView: MetaEntry[];
            let adjustedPos = pos;

            if (newRefCount === 0) {
              // Remove the old entry
              newView = view.toSpliced(oldPos, 1) as MetaEntry[];
              adjustedPos = oldPos < pos ? pos - 1 : pos;
            } else {
              // Leave a copy with decremented refCount at old position
              const oldEntryCopy: MetaEntry = {
                ...oldEntry,
                [refCountSymbol]: newRefCount,
              };
              newView = view.with(oldPos, oldEntryCopy) as MetaEntry[];
            }

            if (found) {
              // Entry already exists at new position, increment its refCount
              const existingEntry = newView[adjustedPos];
              const editedEntry = applyEdit(
                existingEntry,
                change,
                schema,
                withIDs,
              );
              const updatedEntry: MetaEntry = {
                ...editedEntry,
                [refCountSymbol]: editedEntry[refCountSymbol] + 1,
              };
              return {
                ...parentEntry,
                [relationship]: newView.with(adjustedPos, updatedEntry),
              };
            } else {
              // Insert at new position with refCount 1
              const editedEntry = applyEdit(oldEntry, change, schema, withIDs);
              const movedEntry: MetaEntry = {
                ...editedEntry,
                [refCountSymbol]: 1,
              };
              return {
                ...parentEntry,
                [relationship]: newView.toSpliced(adjustedPos, 0, movedEntry),
              };
            }
          }
        } else {
          // Position could not have changed, so simply edit in place.
          const {pos, found} = binarySearch(
            view,
            change.oldNode.row,
            schema.compareRows,
          );
          assert(found, 'node does not exist');
          const newEntry = applyEdit(view[pos], change, schema, withIDs);
          return {...parentEntry, [relationship]: view.with(pos, newEntry)};
        }
      }
    }
    default:
      unreachable(change);
  }
}

function applyEdit(
  existing: MetaEntry,
  change: EditViewChange,
  schema: SourceSchema,
  withIDs: boolean,
): MetaEntry {
  const newEntry: MetaEntry = {
    ...existing,
    ...change.node.row,
  };
  if (withIDs) {
    newEntry[idSymbol] = makeID(change.node.row, schema);
  }
  return newEntry;
}

/**
 * Initializes relationships on a new entry based on the node's relationships.
 * Returns a new entry with relationships added, or the same entry if no relationships.
 */
function initializeRelationships(
  entry: MetaEntry,
  node: Node,
  schema: SourceSchema,
  childFormats: Record<string, Format>,
  withIDs: boolean,
): MetaEntry {
  let result = entry;
  for (const [relationship, children] of Object.entries(node.relationships)) {
    const childSchema = must(schema.relationships[relationship]);
    const childFormat = childFormats[relationship];
    if (childFormat === undefined) {
      continue;
    }

    const newView = childFormat.singular ? undefined : ([] as MetaEntry[]);
    // Only spread if we haven't already
    if (result === entry) {
      result = {...entry, [relationship]: newView};
    } else {
      result[relationship] = newView;
    }

    for (const childNode of skipYields(children())) {
      result = applyChange(
        result,
        {type: 'add', node: childNode},
        childSchema,
        relationship,
        childFormat,
        withIDs,
      ) as MetaEntry;
    }
  }
  return result;
}

function add(
  row: Row,
  view: MetaEntryList,
  schema: SourceSchema,
  withIDs: boolean,
): {newEntry: MetaEntry | undefined; newView: MetaEntry[]} {
  const {pos, found} = binarySearch(view, row, schema.compareRows);

  if (found) {
    // Entry exists, increment refCount
    const existing = view[pos];
    const updated: MetaEntry = {
      ...existing,
      [refCountSymbol]: existing[refCountSymbol] + 1,
    };
    return {
      newEntry: undefined,
      newView: view.with(pos, updated) as MetaEntry[],
    };
  }
  const newEntry = makeNewMetaEntry(row, schema, withIDs, 1);
  return {
    newEntry,
    newView: view.toSpliced(pos, 0, newEntry) as MetaEntry[],
  };
}

function removeAndUpdateRefCount(
  view: MetaEntryList,
  row: Row,
  compareRows: Comparator,
): MetaEntry[] {
  const {pos, found} = binarySearch(view, row, compareRows);
  assert(found, 'node does not exist');
  const oldEntry = view[pos];
  const rc = oldEntry[refCountSymbol];
  if (rc === 1) {
    return view.toSpliced(pos, 1) as MetaEntry[];
  }
  const newEntry: MetaEntry = {
    ...oldEntry,
    [refCountSymbol]: rc - 1,
  };
  return view.with(pos, newEntry) as MetaEntry[];
}

// TODO: Do not return an object. It puts unnecessary pressure on the GC.
function binarySearch(
  view: MetaEntryList,
  target: Row,
  comparator: Comparator,
) {
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

function getChildEntryList(
  parentEntry: Entry,
  relationship: string,
): MetaEntryList {
  const view = parentEntry[relationship];
  assertArray(view);
  return view as MetaEntryList;
}

function assertMetaEntry(v: unknown): asserts v is MetaEntry {
  assertNumber((v as Partial<MetaEntry>)[refCountSymbol]);
}

function getSingularEntry(parentEntry: Entry, relationship: string): MetaEntry {
  const e = parentEntry[relationship];
  assertNumber((e as Partial<MetaEntry>)[refCountSymbol]);
  return e as MetaEntry;
}

function makeNewMetaEntry(
  row: Row,
  schema: SourceSchema,
  withIDs: boolean,
  rc: number,
): MetaEntry {
  if (withIDs) {
    return {...row, [refCountSymbol]: rc, [idSymbol]: makeID(row, schema)};
  }
  return {...row, [refCountSymbol]: rc};
}
function makeID(row: Row, schema: SourceSchema) {
  // optimization for case of non-compound primary key
  if (schema.primaryKey.length === 1) {
    return JSON.stringify(row[schema.primaryKey[0]]);
  }
  return JSON.stringify(schema.primaryKey.map(k => row[k]));
}
