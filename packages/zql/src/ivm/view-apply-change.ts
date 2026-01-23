import {assert, unreachable} from '../../../shared/src/asserts.ts';
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

/*
 * Type Cast Safety Notes:
 *
 * This file uses `as` casts in specific, controlled scenarios:
 *
 * 1. MetaEntry casts: All entries in the view tree are MetaEntry instances
 *    (created via makeNewMetaEntry with refCountSymbol). We cast Entry -> MetaEntry
 *    because we control all entry creation paths.
 *
 * 2. MetaEntryList casts: Child relationship arrays contain MetaEntry instances.
 *    We cast Entry[] -> MetaEntryList for the same reason.
 *
 * 3. Mutable array casts: TypeScript's `with()` and `toSpliced()` return
 *    `readonly T[]` even on mutable arrays. We cast to mutable because we
 *    control array creation and know they're mutable.
 *
 * 4. Row casts in binarySearch: MetaEntry contains all Row properties plus
 *    symbol keys. The comparator only accesses string-indexed properties,
 *    so casting MetaEntry -> Row is safe.
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
 * Applies a change to the view immutably. Returns a new Entry if any changes
 * were made, or the same Entry if nothing changed. Unchanged row objects keep
 * their identity, enabling React.memo with shallow comparison to work effectively.
 *
 * ## Immutable Update Propagation (Leaf to Root)
 *
 * When a change occurs deep in the tree, new objects are created along the path
 * from the change location back to the root. Sibling subtrees are not copied:
 * they keep their original object references.
 *
 * Example: editing row C in the "items" relationship:
 *
 *   BEFORE                              AFTER
 *   ------                              -----
 *   root {                              root' {           <-- new object
 *     users: [A, B],    ─────────────>    users: [A, B],  <-- SAME array ref
 *     items: [C, D, E]                    items: [C', D, E]  <-- new array
 *   }                                   }
 *
 *   Legend:
 *     root  -> root'  : parent replaced (spread creates new object)
 *     [A,B]           : unchanged, same reference (users === users)
 *     [C,D,E] -> [C',D,E] : new array via .with(), but D and E are same refs
 *     C -> C'         : the edited row, new object with updated fields
 *
 * This pattern enables React.memo optimization: unchanged subtrees keep their
 * identity, so React skips re-rendering components whose props haven't changed.
 *
 * The recursion flows DOWN to find the target, then new objects are created
 * on the way back UP:
 *
 *   applyChange(root, childChange)
 *       │
 *       ▼ recurse down to find target
 *   applyChange(items[0], editChange)
 *       │
 *       ▼ create new entry C'
 *   return C'
 *       │
 *       ▲ bubble up, creating new containers
 *   return {...root, items: items.with(0, C')}
 *       │
 *       ▲
 *   return root'  (new root with updated items)
 *
 * ## Object Identity Preservation (Reference Equality)
 *
 * The key insight for React.memo optimization is that unchanged objects keep
 * their exact memory reference. Here's the same example with object addresses:
 *
 *   BEFORE (memory addresses)          AFTER (memory addresses)
 *   -------------------------          ------------------------
 *   root    @0x100 ─┬─ users @0x200    root'   @0x400 ─┬─ users @0x200  (SAME!)
 *                   │    ├─ A @0x210                   │    ├─ A @0x210  (SAME!)
 *                   │    └─ B @0x220                   │    └─ B @0x220  (SAME!)
 *                   │                                  │
 *                   └─ items @0x300    (changed)       └─ items @0x500  (NEW)
 *                        ├─ C @0x310  ─────────────────────> C' @0x510  (NEW)
 *                        ├─ D @0x320                        ├─ D @0x320  (SAME!)
 *                        └─ E @0x330                        └─ E @0x330  (SAME!)
 *
 *   Reference equality checks:
 *     root === root'           // false (new object)
 *     root.users === root'.users   // TRUE  (same array, same address)
 *     root.users[0] === root'.users[0]  // TRUE  (A unchanged)
 *     root.items === root'.items   // false (new array)
 *     root.items[1] === root'.items[1]  // TRUE  (D unchanged)
 *     root.items[0] === root'.items[0]  // false (C was edited)
 *
 * This means React.memo components receiving unchanged subtrees (users, A, B,
 * D, E) will skip re-rendering because props === prevProps is true.
 *
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
    // ─────────────────────────────────────────────────────────────────────────
    // ADD: Insert a new row into the view (or increment refCount if duplicate)
    //
    // Two paths based on relationship cardinality:
    //   singular (one-to-one): parentEntry[relationship] = entry
    //   plural (one-to-many):  parentEntry[relationship] = [..., entry, ...]
    //
    // RefCount tracks duplicates: the same row can appear multiple times via
    // different query paths. Instead of storing duplicates, we increment
    // refCount and only remove when refCount reaches 0.
    // ─────────────────────────────────────────────────────────────────────────
    case 'add': {
      if (singular) {
        const oldEntry = parentEntry[relationship] as MetaEntry | undefined;
        if (oldEntry !== undefined) {
          // Row already exists. For singular relationships, this means a
          // duplicate add (same row reached via different query paths).
          // Increment refCount rather than storing duplicates.
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
          // First time seeing this row. Create entry with refCount=1,
          // then recursively initialize any nested relationships.
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
        // Plural relationship: maintain a sorted array of entries.
        // Binary search finds insert position (or existing entry if duplicate).
        const view = parentEntry[relationship] as MetaEntryList;
        const {newEntry, newView} = add(
          change.node.row,
          view,
          schema,
          withIDs,
        );

        if (newEntry) {
          // Newly inserted entry (not a duplicate). Initialize its child
          // relationships, which may recursively add more entries.
          const initializedEntry = initializeRelationships(
            newEntry,
            change.node,
            schema,
            childFormats,
            withIDs,
          );
          // Replace entry in view if initializeRelationships added children
          // (it returns a new object when children are added).
          if (initializedEntry !== newEntry) {
            const idx = newView.indexOf(newEntry);
            return {
              ...parentEntry,
              [relationship]: (newView as MetaEntry[]).with(idx, initializedEntry),
            };
          }
        }
        return {...parentEntry, [relationship]: newView};
      }
    }
    // ─────────────────────────────────────────────────────────────────────────
    // REMOVE: Delete a row from the view (or decrement refCount if duplicated)
    //
    // Mirror of ADD: decrement refCount, and only physically remove the entry
    // when refCount reaches 0. This ensures rows added multiple times via
    // different query paths are only removed when all paths are gone.
    // ─────────────────────────────────────────────────────────────────────────
    case 'remove': {
      if (singular) {
        const oldEntry = parentEntry[relationship] as MetaEntry | undefined;
        assert(oldEntry !== undefined, 'node does not exist');
        const rc = oldEntry[refCountSymbol];
        if (rc === 1) {
          // Last reference removed. Set relationship to undefined.
          return {...parentEntry, [relationship]: undefined};
        }
        // Other references remain. Decrement refCount but keep entry.
        const newEntry: MetaEntry = {
          ...oldEntry,
          [refCountSymbol]: rc - 1,
        };
        return {...parentEntry, [relationship]: newEntry};
      } else {
        // Plural relationship: find entry via binary search, then either
        // remove it entirely (refCount=1) or decrement refCount.
        const view = parentEntry[relationship] as MetaEntryList;
        const newView = removeAndUpdateRefCount(
          view,
          change.node.row,
          schema.compareRows,
        );
        return {...parentEntry, [relationship]: newView};
      }
    }
    // ─────────────────────────────────────────────────────────────────────────
    // CHILD: Propagate a change from a nested relationship up to this level
    //
    // This is the recursive case that enables deep updates. A change in a
    // grandchild bubbles up through each level, creating new parent objects
    // along the path (the "leaf to root" propagation pattern).
    //
    // Example: editing a comment on a post
    //   applyChange(root, {type:'child', child:{relationshipName:'posts', change:...}})
    //     └─> applyChange(posts[i], {type:'child', child:{relationshipName:'comments', change:...}})
    //           └─> applyChange(comments[j], {type:'edit', ...})
    //                 └─> return comments[j]'  (new comment object)
    //           └─> return posts[i]'  (new post with updated comments)
    //     └─> return root'  (new root with updated posts)
    // ─────────────────────────────────────────────────────────────────────────
    case 'child': {
      const childSchema = must(
        schema.relationships[change.child.relationshipName],
      );
      const childFormat = format.relationships[change.child.relationshipName];
      if (childFormat === undefined) {
        // Child relationship not included in view format. Nothing to update.
        return parentEntry;
      }

      if (singular) {
        const existing = parentEntry[relationship] as MetaEntry;
        const newExisting = applyChange(
          existing,
          change.child.change,
          childSchema,
          change.child.relationshipName,
          childFormat,
          withIDs,
        );
        // Preserve identity if child didn't change (enables React.memo).
        if (newExisting === existing) {
          return parentEntry;
        }
        return {...parentEntry, [relationship]: newExisting};
      } else {
        // Find the target row in the sorted array via binary search.
        const view = parentEntry[relationship] as MetaEntryList;
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
        return {
          ...parentEntry,
          [relationship]: (view as MetaEntry[]).with(pos, newExisting as MetaEntry),
        };
      }
    }
    // ─────────────────────────────────────────────────────────────────────────
    // EDIT: Update row fields in place (or move if sort key changed)
    //
    // Most complex case because an edit can change sort keys, requiring the
    // row to move to a new position in the sorted array. With refCount > 1,
    // we must handle the case where multiple references exist:
    //
    //   refCount=1: Simple move (remove from old pos, insert at new pos)
    //   refCount>1: Leave a "ghost" at old pos with decremented refCount,
    //               then insert/merge at new position
    //
    // Visual example (refCount=2, row moves from pos 1 to pos 3):
    //
    //   BEFORE:  [A, B(rc=2), C, D]     B appears via 2 query paths
    //                ↑
    //   AFTER:   [A, B(rc=1), C, B'(rc=1), D]
    //                ↑ ghost      ↑ moved+edited
    //
    // As more edits arrive for the duplicate, the ghost's refCount decrements
    // until it reaches 0 and is removed.
    // ─────────────────────────────────────────────────────────────────────────
    case 'edit': {
      if (singular) {
        // Singular: just apply the edit, no position concerns.
        const existing = parentEntry[relationship] as MetaEntry;
        const newEntry = applyEdit(existing, change, schema, withIDs);
        return {...parentEntry, [relationship]: newEntry};
      } else {
        const view = parentEntry[relationship] as MetaEntryList;
        // Check if sort key changed by comparing old vs new row.
        // If compareRows returns 0, position is unchanged.
        if (schema.compareRows(change.oldNode.row, change.node.row) !== 0) {
          // Sort key changed. Row may need to move to maintain sorted order.
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
          // Optimization: if refCount=1 and the row would land at or adjacent
          // to its current position, we can edit in place (no actual move).
          // pos === oldPos: would insert at same spot
          // pos - 1 === oldPos: after removing oldPos, new pos would be same
          if (
            oldEntry[refCountSymbol] === 1 &&
            (pos === oldPos || pos - 1 === oldPos)
          ) {
            const newEntry = applyEdit(oldEntry, change, schema, withIDs);
            return {
              ...parentEntry,
              [relationship]: (view as MetaEntry[]).with(oldPos, newEntry),
            };
          } else {
            // Row must actually move. Handle refCount > 1 by leaving a "ghost"
            // at the old position with decremented refCount.
            const newRefCount = oldEntry[refCountSymbol] - 1;
            let newView: MetaEntry[];
            let adjustedPos = pos;

            if (newRefCount === 0) {
              // Last reference at old position. Remove entirely.
              newView = (view as MetaEntry[]).toSpliced(oldPos, 1);
              // Adjust target position since we removed an element before it.
              adjustedPos = oldPos < pos ? pos - 1 : pos;
            } else {
              // Other references remain. Leave ghost with decremented refCount.
              const oldEntryCopy: MetaEntry = {
                ...oldEntry,
                [refCountSymbol]: newRefCount,
              };
              newView = (view as MetaEntry[]).with(oldPos, oldEntryCopy);
            }

            if (found) {
              // Row already exists at new position (another duplicate).
              // Merge by incrementing that entry's refCount.
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
                [relationship]: (newView as MetaEntry[]).with(adjustedPos, updatedEntry),
              };
            } else {
              // No existing entry at new position. Insert fresh with refCount=1.
              const editedEntry = applyEdit(oldEntry, change, schema, withIDs);
              const movedEntry: MetaEntry = {
                ...editedEntry,
                [refCountSymbol]: 1,
              };
              return {
                ...parentEntry,
                [relationship]: (newView as MetaEntry[]).toSpliced(adjustedPos, 0, movedEntry),
              };
            }
          }
        } else {
          // Sort key unchanged. Edit in place without moving.
          const {pos, found} = binarySearch(
            view,
            change.oldNode.row,
            schema.compareRows,
          );
          assert(found, 'node does not exist');
          const newEntry = applyEdit(view[pos], change, schema, withIDs);
          return {...parentEntry, [relationship]: (view as MetaEntry[]).with(pos, newEntry)};
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
      // applyChange preserves MetaEntry structure when input is MetaEntry
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
      newView: (view as MetaEntry[]).with(pos, updated),
    };
  }
  const newEntry = makeNewMetaEntry(row, schema, withIDs, 1);
  return {
    newEntry,
    newView: (view as MetaEntry[]).toSpliced(pos, 0, newEntry),
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
    return (view as MetaEntry[]).toSpliced(pos, 1);
  }
  const newEntry: MetaEntry = {
    ...oldEntry,
    [refCountSymbol]: rc - 1,
  };
  return (view as MetaEntry[]).with(pos, newEntry);
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
    // MetaEntry contains all Row properties (plus symbol keys for internal tracking).
    // The comparator only accesses string-indexed properties, so this is safe.
    // We extract the Row portion to satisfy the Comparator type signature.
    const midEntry = view[mid];
    const comparison = comparator(midEntry as Row, target);
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
