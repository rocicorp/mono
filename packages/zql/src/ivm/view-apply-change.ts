import {
  assert,
  assertArray,
  assertObject,
  unreachable,
} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {Writable} from '../../../shared/src/writable.ts';
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
      let newEntry: Writable<Entry>;

      let rc = 1;
      if (singular) {
        const oldEntry = parentEntry[relationship] as Entry | undefined;
        if (oldEntry !== undefined) {
          assert(
            schema.compareRows(oldEntry, change.node.row) === 0,
            'single output already exists',
          );
          // adding same again.
          rc = must(refCountMap.get(oldEntry)) + 1;
          refCountMap.delete(oldEntry);
        }

        newEntry = makeNewEntryWithRefCount(change.node.row, rc, refCountMap);

        (parentEntry as Writable<Entry>)[relationship] = newEntry;
      } else {
        newEntry = makeNewEntryAndInsert(
          change.node.row,
          refCountMap,
          getChildEntryList(parentEntry, relationship),
          schema.compareRows,
        );
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
          (parentEntry as Writable<Entry>)[relationship] = undefined;
        } else {
          refCountMap.set(oldEntry, rc - 1);
        }
      } else {
        removeAndUpdateRefCount(
          refCountMap,
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
      let existing: Entry;
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
          refCountMap,
        );
      }
      break;
    }
    case 'edit': {
      if (singular) {
        const existing = parentEntry[relationship];
        assertEntry(existing);
        const rc = must(refCountMap.get(existing));
        const newEntry = makeNewEntryWithRefCount(
          {...existing, ...change.node.row},
          rc,
          refCountMap,
        );
        refCountMap.delete(existing);
        (parentEntry as Writable<Entry>)[relationship] = newEntry;
      } else {
        const view = parentEntry[relationship];
        assertEntryList(view);
        // If the order changed due to the edit, we need to remove and reinsert.
        if (schema.compareRows(change.oldNode.row, change.node.row) === 0) {
          const {pos, found} = binarySearch(
            view,
            change.oldNode.row,
            schema.compareRows,
          );
          assert(found, 'node does not exist');
          const oldEntry = view[pos];
          const rc = must(refCountMap.get(oldEntry));
          refCountMap.delete(oldEntry);

          const newEntry = makeEntryPreserveRelationships(
            change.node.row,
            oldEntry,
            format.relationships,
            rc,
            refCountMap,
          );

          (view as Writable<EntryList>)[pos] = newEntry;
        } else {
          // Remove
          const oldEntry = removeAndUpdateRefCount(
            refCountMap,
            view,
            change.oldNode.row,
            schema.compareRows,
          );

          // Insert
          insertAndSetRefCount(
            refCountMap,
            view,
            change.node.row,
            oldEntry,
            format.relationships,
            schema.compareRows,
          );
        }
      }

      break;
    }
    default:
      unreachable(change);
  }
}

function makeNewEntryAndInsert(
  newRow: Row,
  refCountMap: RefCountMap,
  view: EntryList,
  compareRows: Comparator,
): Writable<Entry> {
  const {pos, found} = binarySearch(view, newRow, compareRows);

  let deleteCount = 0;
  let rc = 1;
  if (found) {
    deleteCount = 1;
    rc = must(refCountMap.get(view[pos])) + 1;
    refCountMap.delete(view[pos]);
  }

  const newEntry = makeNewEntryWithRefCount(newRow, rc, refCountMap);

  (view as Writable<EntryList>).splice(pos, deleteCount, newEntry);

  return newEntry;
}

function insertAndSetRefCount(
  refCountMap: RefCountMap,
  view: EntryList,
  newRow: Row,
  oldEntry: Entry,
  relationships: {[key: string]: Format},
  compareRows: Comparator,
): void {
  const {pos, found} = binarySearch(view, newRow, compareRows);

  let deleteCount = 0;
  let rc = 1;
  if (found) {
    deleteCount = 1;
    rc = must(refCountMap.get(view[pos])) + 1;
    refCountMap.delete(view[pos]);
  }

  const newEntry = makeEntryPreserveRelationships(
    newRow,
    oldEntry,
    relationships,
    rc,
    refCountMap,
  );

  (view as Writable<EntryList>).splice(pos, deleteCount, newEntry);
}

function removeAndUpdateRefCount(
  refCountMap: RefCountMap,
  view: EntryList,
  row: Row,
  compareRows: Comparator,
): Entry {
  const {pos, found} = binarySearch(view, row, compareRows);
  assert(found, 'node does not exist');
  const oldEntry = view[pos];
  const rc = must(refCountMap.get(oldEntry));
  if (rc === 1) {
    refCountMap.delete(oldEntry);
    (view as Writable<EntryList>).splice(pos, 1);
  } else {
    refCountMap.set(oldEntry, rc - 1);
  }

  return oldEntry;
}

// TODO: Do not return an object. It puts unnecessary pressure on the GC.
function binarySearch(view: EntryList, target: Row, comparator: Comparator) {
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
  newRow: Row,
  oldEntry: Entry,
  relationships: {[key: string]: Format},
  rc: number,
  refCountMap: RefCountMap,
): Entry {
  const entry = makeNewEntryWithRefCount(newRow, rc, refCountMap);
  for (const relationship in relationships) {
    assert(!(relationship in newRow), 'Relationship already exists');
    entry[relationship] = oldEntry[relationship];
  }
  return entry;
}

function getChildEntryList(
  parentEntry: Entry,
  relationship: string,
): EntryList {
  const view = parentEntry[relationship];
  assertArray(view);
  return view as EntryList;
}

function assertEntryList(v: unknown): asserts v is EntryList {
  assertArray(v);
}

function assertEntry(v: unknown): asserts v is Entry {
  assertObject(v);
}

function getSingularEntry(parentEntry: Entry, relationship: string): Entry {
  const e = parentEntry[relationship];
  assertObject(e);
  return e as Entry;
}

function makeNewEntryWithRefCount(
  row: Row,
  rc: number,
  refCountMap: RefCountMap,
): Writable<Entry> {
  const entry = {...row} as Writable<Entry>;
  refCountMap.set(entry, rc);
  return entry;
}
