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

type ReadonlyMetaEntry = Entry & {
  readonly [refCountSymbol]: number;
  readonly [idSymbol]?: string | undefined;
};

type MutableMetaEntry = Writable<ReadonlyMetaEntry>;

/**
 * MetaEntry is an Entry with internal tracking fields for reference counting
 * and stable identity. All entries in the view tree are MetaEntry instances,
 * created through makeNewMetaEntry().
 */
type MetaEntry<M extends Mutate> = M extends true
  ? MutableMetaEntry
  : ReadonlyMetaEntry;

type MutableArray<M extends Mutate, T> = M extends true ? T[] : readonly T[];

type MetaEntryList<M extends Mutate> = MutableArray<M, MetaEntry<M>>;

type MutableMetaEntryList = MutableMetaEntry[];

/**
 * Node with eagerly-expanded relationships (arrays instead of generators).
 * Used when batching changes to capture source state at push time.
 */
export type ExpandedNode = {
  row: Row;
  relationships: Record<string, ExpandedNode[]>;
};

/**
 * A node for view changes. Can be either:
 * - A lazy Node with generator relationships (original behavior)
 * - An ExpandedNode with pre-computed relationship arrays (for batching)
 */
export type ViewNode = Node | ExpandedNode;

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
  node: ViewNode;
};

export type RemoveViewChange = {
  type: 'remove';
  node: ViewNode;
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
 * Get child nodes from a relationship, handling both lazy (Node) and expanded (ExpandedNode).
 */
function* getChildNodes(
  node: ViewNode,
  relationship: string,
): Generator<ViewNode> {
  const children = node.relationships[relationship];
  if (Array.isArray(children)) {
    // ExpandedNode: already an array
    yield* children;
  } else {
    // Node: lazy generator function
    yield* skipYields(children());
  }
}

type Mutate = boolean;
type WithIDs = boolean;

/**
 * Immutable view update. Returns new Entry on change, same Entry if unchanged.
 * Unchanged entries keep identity, enabling shallow comparison optimizations
 * in UI frameworks (React.memo, Solid's fine-grained reactivity, etc).
 *
 * Propagation: recurse DOWN to find target, copy objects on the way UP.
 * Siblings keep original refs. Only the ancestor path is copied.
 *
 *   root {users:[A,B], items:[C,D,E]}    --edit C-->    root' {users:[A,B], items':[C',D,E]}
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
  mutate = false,
): Entry {
  return applyChangeInternal(
    parentEntry as MetaEntry<typeof mutate>,
    change,
    schema,
    relationship,
    format,
    withIDs,
    mutate,
  );
}

export function applyChangeInternal<M extends Mutate>(
  parentEntry: MetaEntry<M>,
  change: ViewChange,
  schema: SourceSchema,
  relationship: string,
  format: Format,
  withIDs: WithIDs,
  mutate: M,
): MetaEntry<M> {
  if (schema.isHidden) {
    switch (change.type) {
      case 'add':
      case 'remove': {
        let currentParent = parentEntry;
        for (const relationship of Object.keys(change.node.relationships)) {
          const childSchema = must(schema.relationships[relationship]);
          for (const node of getChildNodes(change.node, relationship)) {
            currentParent = applyChangeInternal(
              currentParent,
              {type: change.type, node},
              childSchema,
              relationship,
              format,
              withIDs,
              mutate,
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
        return applyChangeInternal(
          parentEntry,
          change.child.change,
          childSchema,
          relationship,
          format,
          withIDs,
          mutate,
        );
      }
      default:
        unreachable(change);
    }
  }

  const {singular, relationships: childFormats} = format;
  switch (change.type) {
    // ADD: Insert row (rc=1) or increment refCount if duplicate.
    // RefCount tracks when the same row is reachable via multiple edges
    // within a relationship:
    //
    //   issue.labels: [L1, L2]     both point to same creator
    //        │    │                        │
    //        ▼    ▼                        ▼
    //   label.creator ──────────►  [User:rc=2]  (one row, two refs)
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

          const newEntry = incRefCount(mutate, oldEntry);
          return setRelation(mutate, parentEntry, relationship, newEntry);
        } else {
          // New row: create with rc=1, initialize nested relationships
          const newEntry = makeNewMetaEntry(
            change.node.row,
            schema,
            withIDs,
            1,
          );
          initializeRelationshipsForNewEntryIfAny(
            newEntry,
            change.node,
            schema,
            childFormats,
            withIDs,
          );
          return setRelation(true, parentEntry, relationship, newEntry);
        }
      } else {
        // Plural: binary search for position, insert or increment refCount
        const view = getChildEntryList(parentEntry, relationship);
        const {newEntry, newView} = add(
          change.node.row,
          view,
          schema,
          withIDs,
          mutate,
        );

        if (newEntry) {
          initializeRelationshipsForNewEntryIfAny(
            newEntry,
            change.node,
            schema,
            childFormats,
            withIDs,
          );
        }
        return setRelation(mutate, parentEntry, relationship, newView);
      }
    }
    // REMOVE: Decrement refCount, physically remove when rc=0.
    case 'remove': {
      if (singular) {
        const oldEntry = getSingularEntry(parentEntry, relationship);
        const rc = oldEntry[refCountSymbol];
        if (rc === 1) {
          return setRelation(mutate, parentEntry, relationship, undefined);
        }
        const newEntry = decRefCount(mutate, oldEntry);
        return setRelation(mutate, parentEntry, relationship, newEntry);
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        const newView = removeAndUpdateRefCount(
          view,
          change.node.row,
          schema.compareRows,
          mutate,
        );
        return setRelation(mutate, parentEntry, relationship, newView);
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
        const newExisting = applyChangeInternal(
          existing,
          change.child.change,
          childSchema,
          change.child.relationshipName,
          childFormat,
          withIDs,
          mutate,
        );
        // Preserve identity if child didn't change (enables shallow-compare optimizations).
        if (newExisting === existing) {
          return parentEntry;
        }
        return setRelation(mutate, parentEntry, relationship, newExisting);
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
        const newExisting = applyChangeInternal(
          existing,
          change.child.change,
          childSchema,
          change.child.relationshipName,
          childFormat,
          withIDs,
          mutate,
        );
        // Preserve identity if descendant didn't change.
        if (newExisting === existing) {
          return parentEntry;
        }
        // applyChangeInternal preserves MetaEntry structure when input is MetaEntry
        assertMetaEntry(newExisting);
        return setRelation(
          mutate,
          parentEntry,
          relationship,
          arrayWith(mutate, view, pos, newExisting),
        );
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
        const newEntry = applyEdit(existing, change, schema, withIDs, mutate);
        return setRelation(mutate, parentEntry, relationship, newEntry);
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
          // A special case:
          // when refCount is 1 (so the row is being moved without leaving a
          // placeholder behind), and the new pos is the same as the old, or
          // directly after the old (so after the remove of the old it would be
          // in the same pos): the row does not need to be moved, just edited.
          if (
            oldEntry[refCountSymbol] === 1 &&
            (pos === oldPos || pos - 1 === oldPos)
          ) {
            const newEntry = applyEdit(
              oldEntry,
              change,
              schema,
              withIDs,
              mutate,
            );
            return setRelation(
              mutate,
              parentEntry,
              relationship,
              arrayWith(mutate, view, oldPos, newEntry),
            );
          } else {
            // Move the row. If rc > 1, an edit will be received for each ref.
            // On first edit: move row, apply edit, set rc=1. Leave a copy at
            // old pos with decremented rc. As each edit arrives, old copy's rc
            // decrements and new pos rc increments. When copy rc hits 0, remove.
            const newRefCount = oldEntry[refCountSymbol] - 1;
            let newView: MutableMetaEntryList;
            let adjustedPos = pos;

            if (newRefCount === 0) {
              newView = removeAt(mutate, view, oldPos);
              adjustedPos = oldPos < pos ? pos - 1 : pos;
            } else {
              const oldEntryCopy = setRefCount(mutate, oldEntry, newRefCount);
              newView = arrayWith(mutate, view, oldPos, oldEntryCopy);
            }

            if (found) {
              // Merge with existing at new pos
              const existingEntry = newView[adjustedPos];
              const editedEntry = applyEdit(
                existingEntry,
                change,
                schema,
                withIDs,
                mutate,
              );
              const updatedEntry = setRefCount(
                mutate,
                editedEntry,
                existingEntry[refCountSymbol] + 1,
              );
              return setRelation(
                mutate,
                parentEntry,
                relationship,
                arrayWith(mutate, newView, adjustedPos, updatedEntry),
              );
            } else {
              // Insert at new pos with rc=1
              const editedEntry = applyEdit(
                oldEntry,
                change,
                schema,
                withIDs,
                mutate,
              );
              const movedEntry = setRefCount(mutate, editedEntry, 1);
              return setRelation(
                mutate,
                parentEntry,
                relationship,
                insertAt(mutate, newView, adjustedPos, movedEntry),
              );
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
          const newEntry = applyEdit(
            view[pos],
            change,
            schema,
            withIDs,
            mutate,
          );
          return setRelation(
            mutate,
            parentEntry,
            relationship,
            arrayWith(mutate, view, pos, newEntry),
          );
        }
      }
    }
    default:
      unreachable(change);
  }
}

/**
 * Batch apply multiple changes to an Entry tree.
 * For small batches or complex cases, falls back to sequential applyChange.
 * Future optimization: O(N + K) batch processing for large K.
 */
export function applyChanges(
  parentEntry: Entry,
  changes: ViewChange[],
  schema: SourceSchema,
  relationship: string,
  format: Format,
  withIDs: WithIDs = false,
  mutate: Mutate = false,
): Entry {
  let result = parentEntry;
  for (const change of changes) {
    result = applyChange(
      result,
      change,
      schema,
      relationship,
      format,
      withIDs,
      mutate,
    );
  }
  return result;
}

function applyEdit<M extends Mutate>(
  existing: MetaEntry<M>,
  change: EditViewChange,
  schema: SourceSchema,
  withIDs: WithIDs,
  mutate: Mutate,
): MetaEntry<M> {
  const newEntry: MutableMetaEntry =
    // Even for mutate we want to create a new entry if the primary key changed.
    mutate && schema.compareRows(change.oldNode.row, change.node.row) === 0
      ? Object.assign(existing, change.node.row)
      : {...existing, ...change.node.row};

  if (withIDs) {
    return setProperty(
      mutate,
      newEntry,
      idSymbol,
      makeID(change.node.row, schema),
    );
  }
  return newEntry;
}

/**
 * Initialize child relationships on a newly-added entry.
 * Returns the same entry reference if no relationships to initialize,
 * or a new entry (via spread) if relationships were added.
 *
 * New nodes don't exist in the view yet, so we can build in-place (no refs to preserve):
 *
 *   Existing node edit (must path-copy):     New node (can build in-place):
 *
 *   view: [A, B, C]                          view: [A, B, _]
 *              │                                         │
 *         edit C.child                              add C with children
 *              │                                         │
 *              ▼                                         ▼
 *   view': [A, B, C']  ◄─ path-copy          view': [A, B, C{children:[...]}]
 *                         up the tree                    └─ built in-place
 *
 * Hidden schemas still use applyChange to collapse junction levels.
 */
function initializeRelationshipsForNewEntryIfAny(
  entry: MutableMetaEntry,
  node: ViewNode,
  schema: SourceSchema,
  childFormats: Record<string, Format>,
  withIDs: WithIDs,
): void {
  let result = entry;
  for (const relationship of Object.keys(node.relationships)) {
    const childSchema = must(schema.relationships[relationship]);
    const childFormat = childFormats[relationship];
    if (childFormat === undefined) {
      continue;
    }

    // Hidden/singular: use applyChange to handle properly
    if (childSchema.isHidden || childFormat.singular) {
      const newView = childFormat.singular
        ? undefined
        : ([] as MutableMetaEntryList);
      result[relationship] = newView;

      for (const childNode of getChildNodes(node, relationship)) {
        applyChangeInternal(
          result,
          {type: 'add', node: childNode},
          childSchema,
          relationship,
          childFormat,
          withIDs,
          true, // this is a new entry, so we can mutate
        );
      }
    } else {
      // Plural non-hidden: build array in-place for efficiency
      const childArray: MutableMetaEntryList = [];

      for (const childNode of getChildNodes(node, relationship)) {
        const newEntry = makeNewMetaEntry(
          childNode.row,
          childSchema,
          withIDs,
          1,
        );
        const {pos, found} = binarySearch(
          childArray,
          childNode.row,
          childSchema.compareRows,
        );

        if (found) {
          childArray[pos][refCountSymbol]++;
        } else {
          childArray.splice(pos, 0, newEntry);
          initializeRelationshipsForNewEntryIfAny(
            newEntry,
            childNode,
            childSchema,
            childFormat.relationships,
            withIDs,
          );
        }
      }

      result[relationship] = childArray;
    }
  }
}

function add<M extends Mutate>(
  row: Row,
  view: MetaEntryList<M>,
  schema: SourceSchema,
  withIDs: WithIDs,
  mutate: M,
): {
  newEntry: MutableMetaEntry | undefined;
  newView: MutableMetaEntryList;
} {
  const {pos, found} = binarySearch(view, row, schema.compareRows);

  if (found) {
    const existing = view[pos];

    const updated = incRefCount(mutate, existing);
    return {
      newEntry: undefined,
      newView: arrayWith(mutate, view, pos, updated),
    };
  }
  const newEntry = makeNewMetaEntry(row, schema, withIDs, 1);
  return {newEntry, newView: insertAt(mutate, view, pos, newEntry)};
}

function insertAt<M extends Mutate, T>(
  mutate: M,
  array: MutableArray<M, T>,
  index: number,
  item: T,
): T[] {
  if (mutate) {
    (array as T[]).splice(index, 0, item);
    return array as T[];
  }
  return array.toSpliced(index, 0, item);
}

function removeAt<M extends Mutate, T>(
  mutate: M,
  array: MutableArray<M, T>,
  index: number,
): T[] {
  if (mutate) {
    (array as T[]).splice(index, 1);
    return array as T[];
  }
  return array.toSpliced(index, 1);
}

function removeAndUpdateRefCount<M extends Mutate>(
  view: MetaEntryList<M>,
  row: Row,
  compareRows: Comparator,
  mutate: M,
): MutableMetaEntryList {
  const {pos, found} = binarySearch(view, row, compareRows);
  assert(found, 'node does not exist');
  const oldEntry = view[pos];
  const rc = oldEntry[refCountSymbol];
  if (rc === 1) {
    return removeAt(mutate, view, pos);
  }
  const newEntry = decRefCount(mutate, oldEntry);
  return arrayWith(mutate, view, pos, newEntry);
}

function binarySearch(
  view: MetaEntryList<Mutate>,
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
function assertMetaEntry<M extends Mutate>(
  v: unknown,
): asserts v is MetaEntry<M> {
  assertNumber((v as Partial<MetaEntry<M>>)[refCountSymbol]);
}

/** Get singular MetaEntry, throws if missing. */
function getSingularEntry<M extends Mutate>(
  parentEntry: MetaEntry<M>,
  relationship: string,
): MetaEntry<M> {
  const entry = parentEntry[relationship];
  assert(entry !== undefined, 'node does not exist');
  assertMetaEntry(entry);
  return entry;
}

/** Get singular MetaEntry or undefined if not set. */
function getOptionalSingularEntry<M extends Mutate>(
  parentEntry: MetaEntry<M>,
  relationship: string,
): MetaEntry<M> | undefined {
  const entry = parentEntry[relationship];
  if (entry === undefined) {
    return undefined;
  }
  assertMetaEntry(entry);
  return entry;
}

/** Get child array as MetaEntryList. */
function getChildEntryList<M extends Mutate>(
  parentEntry: MetaEntry<M>,
  relationship: string,
): MetaEntryList<M> {
  const view = parentEntry[relationship];
  assertArray(view);
  return view as MetaEntryList<M>;
}

/** Create MetaEntry from row with given refCount. */
function makeNewMetaEntry(
  row: Row,
  schema: SourceSchema,
  withIDs: WithIDs,
  rc: number,
): MutableMetaEntry {
  // This creates a new MetaEntry from a Row. We never mutate Rows.
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

function incRefCount<M extends Mutate>(
  mutate: M,
  entry: MetaEntry<M>,
): MutableMetaEntry {
  return setRefCount(mutate, entry, entry[refCountSymbol] + 1);
}

function decRefCount<M extends Mutate>(
  mutate: M,
  entry: MetaEntry<M>,
): MutableMetaEntry {
  return setRefCount(mutate, entry, entry[refCountSymbol] - 1);
}

function setRefCount<M extends Mutate>(
  mutate: M,
  entry: MetaEntry<M>,
  count: number,
): MutableMetaEntry {
  if (mutate) {
    (entry as MutableMetaEntry)[refCountSymbol] = count;
    return entry;
  }
  return {...entry, [refCountSymbol]: count};
}

function arrayWith<M extends Mutate, T>(
  mutate: M,
  array: MutableArray<M, T>,
  index: number,
  value: T,
): T[] {
  if (mutate) {
    (array as T[])[index] = value;
    return array as T[];
  }
  return array.with(index, value);
}

function setProperty<
  M extends Mutate,
  K extends string | keyof MetaEntry<M>,
  V,
>(
  mutate: M,
  parentEntry: MetaEntry<M>,
  key: K,
  value: V,
): MutableMetaEntry & {[P in K]: V} {
  if (mutate) {
    (parentEntry as {[P in K]: V})[key] = value;
    return parentEntry as MutableMetaEntry & {[P in K]: V};
  }
  return {...parentEntry, [key]: value};
}

const setRelation: <M extends Mutate>(
  mutate: M,
  parentEntry: MetaEntry<M>,
  relationship: string,
  value: Entry | Entry[] | undefined,
) => MutableMetaEntry = setProperty;
