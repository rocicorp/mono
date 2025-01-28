import {
  assert,
  assertArray,
  assertObject,
  assertUndefined,
  unreachable,
} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import type {Comparator, Node} from './data.ts';
import type {SourceSchema} from './schema.ts';
import type {Entry, EntryList, Format} from './view.ts';

export function applyChange(
  parentEntry: Entry,
  change: Change,
  schema: SourceSchema,
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
          for (const node of children()) {
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
        const view = getChildEntryList(parentEntry, relationship);
        const {pos, found} = binarySearch(view, newEntry, schema.compareRows);
        assert(!found, 'node already exists');
        // @ts-expect-error view is readonly
        view.splice(pos, 0, newEntry);
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
        const view = getChildEntryList(parentEntry, relationship);
        const {pos, found} = binarySearch(
          view,
          change.node.row,
          schema.compareRows,
        );
        assert(found, 'node does not exist');
        // @ts-expect-error view is readonly
        view.splice(pos, 1);
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
        );
      }
      break;
    }
    case 'edit': {
      if (singular) {
        assertObject(parentEntry[relationship]);
        parentEntry[relationship] = {
          ...parentEntry[relationship],
          ...change.node.row,
        };
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
          view[pos] = makeEntryPreserveRelationships(
            change.node.row,
            view[pos],
            schema.relationships,
          );
        } else {
          // Remove
          const {pos, found} = binarySearch(
            view,
            change.oldNode.row,
            schema.compareRows,
          );
          assert(found, 'node does not exists');
          const oldEntry = view[pos];
          view.splice(pos, 1);

          // Insert
          {
            const {pos, found} = binarySearch(
              view,
              change.node.row,
              schema.compareRows,
            );
            assert(!found, 'node already exists');
            view.splice(
              pos,
              0,
              makeEntryPreserveRelationships(
                change.node.row,
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
  relationships: {[key: string]: SourceSchema},
): Entry {
  const result: Entry = {...row};
  for (const relationship in relationships) {
    assert(!(relationship in row), 'Relationship already exists');
    result[relationship] = entry[relationship];
  }
  return result;
}

function drainStreams(node: Node) {
  for (const stream of Object.values(node.relationships)) {
    for (const node of stream()) {
      drainStreams(node);
    }
  }
}

function getChildEntryList(
  parentEntry: Entry,
  relationship: string,
): EntryList {
  const view = parentEntry[relationship] as unknown;
  assertArray(view);
  return view as EntryList;
}
