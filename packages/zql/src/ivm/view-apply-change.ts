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

/**
 * MetaEntry is an Entry with internal tracking fields for reference counting
 * and stable identity. All entries in the view tree are MetaEntry instances,
 * created through makeNewMetaEntry().
 */
type MetaEntry = Writable<Entry> & {
  [refCountSymbol]: number;
  [idSymbol]?: string | undefined;
};
type MetaEntryList = MetaEntry[];

/*
 * Runtime assertions via getSingularEntry/getChildEntryList catch bugs early
 * (clear error at violation point vs mysterious NaN refCounts later).
 */

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
 * Immutable view update. Returns new Entry on change, same Entry if unchanged.
 * Unchanged entries keep identity, enabling shallow comparison optimizations
 * in UI frameworks (React.memo, Solid's fine-grained reactivity, etc).
 *
 * Propagation: recurse DOWN to find target, copy objects on the way UP.
 * Siblings keep original refs. Only the ancestor path is copied.
 *
 *   root {users:[A,B], items:[C,D,E]}    --edit C-->    root' {users:[A,B], items:[C',D,E]}
 *         │ same ref        │                                  │              │
 *         └─────────────────┴── unchanged ──────────────┘     new array     C' new, D/E same
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
    // ADD: Insert row (rc=1) or increment refCount if duplicate.
    // RefCount tracks identical rows reached via different query paths.
    //
    //   add(A)      add(A)      remove(A)   remove(A)
    //     ↓           ↓            ↓           ↓
    //   [A:rc=1] → [A:rc=2] → [A:rc=1] → (deleted)
    case 'add': {
      if (singular) {
        const oldEntry = getOptionalSingularEntry(parentEntry, relationship);
        if (oldEntry !== undefined) {
          // Duplicate add: increment refCount
          assert(
            schema.compareRows(oldEntry, change.node.row) === 0,
            `Singular relationship '${relationship}' should not have multiple rows. You may need to declare this relationship with the \`many\` helper instead of the \`one\` helper in your schema.`,
          );
          const newEntry: MetaEntry = {
            ...oldEntry,
            [refCountSymbol]: oldEntry[refCountSymbol] + 1,
          };
          return {...parentEntry, [relationship]: newEntry};
        } else {
          // New row: create with rc=1, initialize nested relationships
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
        // Plural: binary search for position, insert or increment refCount
        const view = getChildEntryList(parentEntry, relationship);
        const {newEntry, newView, pos} = add(
          change.node.row,
          view,
          schema,
          withIDs,
        );

        if (newEntry) {
          // New entry: initialize children, update in place if children added
          const initializedEntry = initializeRelationships(
            newEntry,
            change.node,
            schema,
            childFormats,
            withIDs,
          );
          if (initializedEntry !== newEntry) {
            return {
              ...parentEntry,
              [relationship]: newView.with(pos, initializedEntry),
            };
          }
        }
        return {...parentEntry, [relationship]: newView};
      }
    }
    // REMOVE: Decrement refCount, physically remove when rc=0.
    case 'remove': {
      if (singular) {
        const oldEntry = getSingularEntry(parentEntry, relationship);
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
    // CHILD: Propagate nested change up to this level (leaf-to-root pattern).
    case 'child': {
      const childSchema = must(
        schema.relationships[change.child.relationshipName],
      );
      const childFormat = format.relationships[change.child.relationshipName];
      if (childFormat === undefined) {
        return parentEntry; // Relationship not in view format
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
        // Preserve identity if child didn't change (enables shallow-compare optimizations).
        if (newExisting === existing) {
          return parentEntry;
        }
        return {...parentEntry, [relationship]: newExisting};
      } else {
        // Find the target row in the sorted array via binary search.
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
        // Preserve identity if descendant didn't change.
        if (newExisting === existing) {
          return parentEntry;
        }
        // applyChange preserves MetaEntry structure when input is MetaEntry
        assertMetaEntry(newExisting);
        return {
          ...parentEntry,
          [relationship]: view.with(pos, newExisting),
        };
      }
    }
    // EDIT: Update row fields. If sort key changes, row may move position.
    //
    // Position change with rc>1 (two query paths reach same row):
    //
    //   Before: [A, B(rc=2), C, D]          After: [A, B(rc=1), C, B'(rc=1), D]
    //                │                                  │            │
    //           path1 + path2                      path1 ghost    path2 moved
    //
    // Why ghost stays: path1 still expects B at old position.
    // B' appears where path2's sort order places it.
    case 'edit': {
      if (singular) {
        const existing = getSingularEntry(parentEntry, relationship);
        const newEntry = applyEdit(existing, change, schema, withIDs);
        return {...parentEntry, [relationship]: newEntry};
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        // Sort key changed: row may need to move
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
          // rc=1 and same/adjacent pos: edit in place, no move needed
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
            // Row moves: leave ghost at old pos if rc>1
            const newRefCount = oldEntry[refCountSymbol] - 1;
            let newView: MetaEntry[];
            let adjustedPos = pos;

            if (newRefCount === 0) {
              newView = view.toSpliced(oldPos, 1);
              adjustedPos = oldPos < pos ? pos - 1 : pos;
            } else {
              const oldEntryCopy: MetaEntry = {
                ...oldEntry,
                [refCountSymbol]: newRefCount,
              };
              newView = view.with(oldPos, oldEntryCopy);
            }

            if (found) {
              // Merge with existing at new pos
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
              // Insert at new pos with rc=1
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
          // Sort key unchanged: edit in place
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

/** Initialize child relationships on a new entry. Hidden schemas use applyChange
 * to collapse the junction level; non-hidden build arrays in-place for efficiency. */
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

    // Hidden/singular: use applyChange to handle properly
    if (childSchema.isHidden || childFormat.singular) {
      const newView = childFormat.singular ? undefined : ([] as MetaEntry[]);
      if (result === entry) {
        result = {...entry, [relationship]: newView};
      } else {
        result[relationship] = newView;
      }

      for (const childNode of skipYields(children())) {
        const newResult = applyChange(
          result,
          {type: 'add', node: childNode},
          childSchema,
          relationship,
          childFormat,
          withIDs,
        );
        assertMetaEntry(newResult);
        result = newResult;
      }
    } else {
      // Plural non-hidden: build array in-place for efficiency
      const childArray: MetaEntry[] = [];

      for (const childNode of skipYields(children())) {
        const newEntry = makeNewMetaEntry(childNode.row, childSchema, withIDs, 1);
        const {pos, found} = binarySearch(
          childArray,
          childNode.row,
          childSchema.compareRows,
        );

        if (found) {
          const existing = childArray[pos];
          childArray[pos] = {
            ...existing,
            [refCountSymbol]: existing[refCountSymbol] + 1,
          };
        } else {
          childArray.splice(pos, 0, newEntry);
          const initializedEntry = initializeRelationships(
            newEntry,
            childNode,
            childSchema,
            childFormat.relationships,
            withIDs,
          );
          if (initializedEntry !== newEntry) {
            childArray[pos] = initializedEntry;
          }
        }
      }

      if (result === entry) {
        result = {...entry, [relationship]: childArray};
      } else {
        result[relationship] = childArray;
      }
    }
  }
  return result;
}

function add(
  row: Row,
  view: MetaEntryList,
  schema: SourceSchema,
  withIDs: boolean,
): {newEntry: MetaEntry | undefined; newView: MetaEntry[]; pos: number} {
  const {pos, found} = binarySearch(view, row, schema.compareRows);

  if (found) {
    const existing = view[pos];
    const updated: MetaEntry = {
      ...existing,
      [refCountSymbol]: existing[refCountSymbol] + 1,
    };
    return {newEntry: undefined, newView: view.with(pos, updated), pos};
  }
  const newEntry = makeNewMetaEntry(row, schema, withIDs, 1);
  return {newEntry, newView: view.toSpliced(pos, 0, newEntry), pos};
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
    return view.toSpliced(pos, 1);
  }
  const newEntry: MetaEntry = {
    ...oldEntry,
    [refCountSymbol]: rc - 1,
  };
  return view.with(pos, newEntry);
}

function binarySearch(
  view: MetaEntryList,
  target: Row,
  comparator: Comparator,
) {
  let low = 0;
  let high = view.length - 1;
  while (low <= high) {
    const mid = (low + high) >>> 1;
    // MetaEntry has all Row props; comparator only reads string keys
    const comparison = comparator(view[mid] as Row, target);
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

/** Assert value is MetaEntry (has refCountSymbol). */
function assertMetaEntry(v: unknown): asserts v is MetaEntry {
  assertNumber((v as Partial<MetaEntry>)[refCountSymbol]);
}

/** Get singular MetaEntry, throws if missing. */
function getSingularEntry(parentEntry: Entry, relationship: string): MetaEntry {
  const entry = parentEntry[relationship];
  assert(entry !== undefined, 'node does not exist');
  assertMetaEntry(entry);
  return entry;
}

/** Get singular MetaEntry or undefined if not set. */
function getOptionalSingularEntry(
  parentEntry: Entry,
  relationship: string,
): MetaEntry | undefined {
  const entry = parentEntry[relationship];
  if (entry === undefined) {
    return undefined;
  }
  assertMetaEntry(entry);
  return entry;
}

/** Get child array as MetaEntryList. */
function getChildEntryList(
  parentEntry: Entry,
  relationship: string,
): MetaEntryList {
  const view = parentEntry[relationship];
  assertArray(view);
  return view as MetaEntryList;
}

/** Create MetaEntry from row with given refCount. */
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
