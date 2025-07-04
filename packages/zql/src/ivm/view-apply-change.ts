import {
  assert,
  assertArray,
  assertNumber,
  unreachable,
} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {drainStreams, type Comparator, type Node} from './data.ts';
import type {SourceSchema} from './schema.ts';
import type {Entry, Format} from './view.ts';

export const refCountSymbol = Symbol('rc');
export const idSymbol = Symbol('id');

type RCEntry = Writable<Entry> & {
  [refCountSymbol]: number;
  [idSymbol]: string;
};
type RCEntryList = RCEntry[];

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

export function applyChange(
  parentEntry: Entry,
  change: ViewChange,
  schema: SourceSchema,
  relationship: string,
  format: Format,
  withIDs = false,
): void {
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
              withIDs,
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
          withIDs,
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
      let newEntry: RCEntry | undefined;

      if (singular) {
        const oldEntry = parentEntry[relationship] as RCEntry | undefined;
        if (oldEntry !== undefined) {
          assert(
            schema.compareRows(oldEntry, change.node.row) === 0,
            `Singular relationship '${relationship}' should not have multiple rows. You may need to declare this relationship with the \`many\` helper instead of the \`one\` helper in your schema.`,
          );
          // adding same again.
          oldEntry[refCountSymbol]++;
        } else {
          newEntry = makeNewEntryWithRefCount(
            change.node.row,
            schema,
            withIDs,
            1,
          );

          (parentEntry as Writable<Entry>)[relationship] = newEntry;
        }
      } else {
        newEntry = add(
          change.node.row,
          getChildEntryList(parentEntry, relationship),
          schema,
          withIDs,
        );
      }

      if (newEntry) {
        for (const [relationship, children] of Object.entries(
          change.node.relationships,
        )) {
          // TODO: Is there a flag to make TypeScript complain that dictionary access might be undefined?
          const childSchema = must(schema.relationships[relationship]);
          const childFormat = childFormats[relationship];
          if (childFormat === undefined) {
            continue;
          }

          const newView = childFormat.singular
            ? undefined
            : ([] as RCEntryList);
          newEntry[relationship] = newView;

          for (const node of children()) {
            applyChange(
              newEntry,
              {type: 'add', node},
              childSchema,
              relationship,
              childFormat,
              withIDs,
            );
          }
        }
      }
      break;
    }
    case 'remove': {
      if (singular) {
        const oldEntry = parentEntry[relationship] as RCEntry | undefined;
        assert(oldEntry !== undefined, 'node does not exist');
        const rc = oldEntry[refCountSymbol];
        if (rc === 1) {
          (parentEntry as Writable<Entry>)[relationship] = undefined;
        }
        oldEntry[refCountSymbol]--;
      } else {
        removeAndUpdateRefCount(
          getChildEntryList(parentEntry, relationship),
          change.node.row,
          schema.compareRows,
        );
      }
      // Needed to ensure cleanup of operator state is fully done.
      drainStreams(change.node);
      break;
    }
    case 'child': {
      let existing: RCEntry;
      if (singular) {
        existing = getSingularEntry(parentEntry, relationship);
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
          withIDs,
        );
      }
      break;
    }
    case 'edit': {
      if (singular) {
        const existing = parentEntry[relationship];
        assertRCEntry(existing);
        applyEdit(existing, change, schema, withIDs);
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        // If the order changed due to the edit, we need to remove and reinsert.
        if (schema.compareRows(change.oldNode.row, change.node.row) !== 0) {
          // Remove
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
          if (oldPos === pos || oldPos + 1 === pos) {
            applyEdit(oldEntry, change, schema, withIDs);
          } else {
            oldEntry[refCountSymbol]--;
            let adjustedPos = pos;
            if (oldEntry[refCountSymbol] === 0) {
              view.splice(oldPos, 1);
              adjustedPos = oldPos < pos ? pos - 1 : pos;
            }

            let entryToEdit;
            if (found) {
              entryToEdit = view[adjustedPos];
            } else {
              view.splice(adjustedPos, 0, oldEntry);
              entryToEdit = oldEntry;
              if (oldEntry[refCountSymbol] > 0) {
                const oldEntryCopy = {...oldEntry};
                view[oldPos] = oldEntryCopy;
              }
            }
            entryToEdit[refCountSymbol]++;
            applyEdit(entryToEdit, change, schema, withIDs);
          }
        } else {
          const {pos, found} = binarySearch(
            view,
            change.oldNode.row,
            schema.compareRows,
          );
          assert(found, 'node does not exist');
          applyEdit(view[pos], change, schema, withIDs);
        }
      }

      break;
    }
    default:
      unreachable(change);
  }
}

function applyEdit(
  existing: RCEntry,
  change: EditViewChange,
  schema: SourceSchema,
  withIDs: boolean,
) {
  Object.assign(existing, change.node.row);
  if (withIDs) {
    existing[idSymbol] = makeID(change.node.row, schema);
  }
}

function add(
  row: Row,
  view: RCEntryList,
  schema: SourceSchema,
  withIDs: boolean,
): RCEntry | undefined {
  const {pos, found} = binarySearch(view, row, schema.compareRows);

  if (found) {
    view[pos][refCountSymbol]++;
    return undefined;
  }
  const newEntry = makeNewEntryWithRefCount(row, schema, withIDs, 1);
  view.splice(pos, 0, newEntry);
  return newEntry;
}

function removeAndUpdateRefCount(
  view: RCEntryList,
  row: Row,
  compareRows: Comparator,
): RCEntry {
  const {pos, found} = binarySearch(view, row, compareRows);
  assert(found, 'node does not exist');
  const oldEntry = view[pos];
  const rc = oldEntry[refCountSymbol];
  if (rc === 1) {
    view.splice(pos, 1);
  }
  oldEntry[refCountSymbol]--;

  return oldEntry;
}

// TODO: Do not return an object. It puts unnecessary pressure on the GC.
function binarySearch(view: RCEntryList, target: Row, comparator: Comparator) {
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
): RCEntryList {
  const view = parentEntry[relationship];
  assertArray(view);
  return view as RCEntryList;
}

function assertRCEntry(v: unknown): asserts v is RCEntry {
  assertNumber((v as Partial<RCEntry>)[refCountSymbol]);
}

function getSingularEntry(parentEntry: Entry, relationship: string): RCEntry {
  const e = parentEntry[relationship];
  assertNumber((e as Partial<RCEntry>)[refCountSymbol]);
  return e as RCEntry;
}

function makeNewEntryWithRefCount(
  row: Row,
  schema: SourceSchema,
  withIDs: boolean,
  rc: number,
): RCEntry {
  const id = withIDs ? makeID(row, schema) : '';
  return {...row, [refCountSymbol]: rc, [idSymbol]: id};
}
function makeID(row: Row, schema: SourceSchema) {
  // optimization for case of non-compound primary key
  if (schema.primaryKey.length === 1) {
    return JSON.stringify(row[schema.primaryKey[0]]);
  }
  return JSON.stringify(schema.primaryKey.map(k => row[k]));
}
