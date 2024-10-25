import {
  assert,
  assertArray,
  assertObject,
  assertUndefined,
  unreachable,
} from '../../../../shared/src/asserts.js';
import {must} from '../../../../shared/src/must.js';
import type {Row} from '../../../../zero-protocol/src/data.js';
import type {Change} from './change.js';
import type {Comparator} from './data.js';
import type {TableSchema} from './schema.js';
import type {Entry, EntryList, Format} from './view.js';

export function applyChange(
  parentEntry: Entry,
  change: Change,
  schema: TableSchema,
  relationship: string,
  format: Format,
) {
  if (schema.isHidden) {
    switch (change.type) {
      case 'add':
      case 'remove':
        for (const [relationship, children] of Object.entries(
          change.node.relationships,
        )) {
          const childSchema = must(schema.relationships[relationship]);
          for (const node of children) {
            applyChange(
              parentEntry,
              {type: change.type, node},
              childSchema,
              relationship,
              format,
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
        assertUndefined(
          parentEntry[relationship],
          'single output already exists',
        );
        parentEntry[relationship] = newEntry;
      } else {
        const view = parentEntry[relationship];
        assertArray(view);
        const {pos, found} = binarySearch(view, newEntry, schema.compareRows);
        assert(!found, 'node already exists');
        view.splice(pos, 0, newEntry);
      }
      for (const [relationship, children] of Object.entries(
        change.node.relationships,
      )) {
        // TODO: Is there a flag to make TypeScript complain that dictionary access might be undefined?
        const childSchema = must(schema.relationships[relationship]);
        const childFormat = must(childFormats[relationship]);
        const newView = childFormat.singular ? undefined : ([] as EntryList);
        newEntry[relationship] = newView;
        for (const node of children) {
          applyChange(
            newEntry,
            {type: 'add', node},
            childSchema,
            relationship,
            childFormat,
          );
        }
      }
      break;
    }
    case 'remove': {
      if (singular) {
        assertObject(parentEntry[relationship]);
        parentEntry[relationship] = undefined;
      } else {
        assertArray(parentEntry[relationship]);
        const view = parentEntry[relationship];
        const {pos, found} = binarySearch(
          view,
          change.node.row,
          schema.compareRows,
        );
        assert(found, 'node does not exist');
        view.splice(pos, 1);
      }
      break;
    }
    case 'child': {
      let existing: Entry;
      if (singular) {
        assertObject(parentEntry[relationship]);
        existing = parentEntry[relationship];
      } else {
        assertArray(parentEntry[relationship]);
        const list = parentEntry[relationship];
        const {pos, found} = binarySearch(list, change.row, schema.compareRows);
        assert(found, 'node does not exist');
        existing = list[pos];
      }

      const childSchema = must(
        schema.relationships[change.child.relationshipName],
      );
      const childFormat = must(
        format.relationships[change.child.relationshipName],
      );
      applyChange(
        existing,
        change.child.change,
        childSchema,
        change.child.relationshipName,
        childFormat,
      );
      break;
    }
    case 'edit': {
      if (singular) {
        assertObject(parentEntry[relationship]);
        parentEntry[relationship] = {
          ...parentEntry[relationship],
          ...change.row,
        };
      } else {
        assertArray(parentEntry[relationship]);
        const view = parentEntry[relationship];
        // If the order changed due to the edit, we need to remove and reinsert.
        if (schema.compareRows(change.oldRow, change.row) === 0) {
          const {pos, found} = binarySearch(
            view,
            change.oldRow,
            schema.compareRows,
          );
          assert(found, 'node does not exists');
          view[pos] = makeEntryPreserveRelationships(
            change.row,
            view[pos],
            schema.relationships,
          );
        } else {
          // Remove
          const {pos, found} = binarySearch(
            view,
            change.oldRow,
            schema.compareRows,
          );
          assert(found, 'node does not exists');
          const oldEntry = view[pos];
          view.splice(pos, 1);

          // Insert
          {
            const {pos, found} = binarySearch(
              view,
              change.row,
              schema.compareRows,
            );
            assert(!found, 'node already exists');
            view.splice(
              pos,
              0,
              makeEntryPreserveRelationships(
                change.row,
                oldEntry,
                schema.relationships,
              ),
            );
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
  relationships: {[key: string]: TableSchema},
): Entry {
  const result: Entry = {...row};
  for (const relationship in relationships) {
    assert(!(relationship in row), 'Relationship already exists');
    result[relationship] = entry[relationship];
  }
  return result;
}
