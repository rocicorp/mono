import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {
  makeAddChange,
  makeChildChange,
  makeEditChange,
  makeRemoveChange,
  type Change,
} from './change.ts';
import type {Node} from './data.ts';
import type {InputBase, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

/**
 * # pushAccumulatedChanges
 *
 * Pushes the changes that were accumulated by
 * [fan-out, fan-in] or [ufo, ufi] sub-graphs.
 *
 * This function is called at the end of the sub-graph.
 *
 * The sub-graphs represents `OR`s.
 *
 * Changes that can enter the subgraphs:
 * 1. child (due to exist joins being above the sub-graph)
 * 2. add
 * 3. remove
 * 4. edit
 *
 * # Changes that can exit into `pushAccumulatedChanges`:
 *
 * ## Child
 * If a `child` change enters a sub-graph, it will flow to all branches.
 * Each branch will either:
 * - preserve the `child` change
 * - stop the `child` change (e.g., filter)
 * - convert it to an `add` or `remove` (e.g., exists filter)
 *
 * ## Add
 * If an `add` change enters a sub-graph, it will flow to all branches.
 * Each branch will either:
 * - preserve the `add` change
 * - hide the change (e.g., filter)
 *
 * ## Remove
 * If a `remove` change enters a sub-graph, it will flow to all branches.
 * Each branch will either:
 * - preserve the `remove` change
 * - hide the change (e.g., filter)
 *
 * ## Edit
 * If an `edit` change enters a sub-graph, it will flow to all branches.
 * Each branch will either:
 * - preserve the `edit` change
 * - convert it to an `add` (e.g., filter where old didn't match but new does)
 * - convert it to a `remove` (e.g., filter where old matched but new doesn't)
 *
 * This results in some invariants:
 * - an add coming in will only create adds coming out
 * - a remove coming in will only create removes coming out
 * - an edit coming in can create adds, removes, and edits coming out
 * - a child coming in can create adds, removes, and children coming out
 *
 * # Return of `pushAccumulatedChanges`
 *
 * This function will only push a single change.
 * Given the above invariants, how is this possible?
 *
 * An add that becomes many `adds` results in a single add
 * as the `add` is the same row across all adds. Branches do not change the row.
 *
 * A remove that becomes many `removes` results in a single remove
 * for the same reason.
 *
 * If a child enters and exits, it takes precedence over all other changes.
 * If a child enters and is converted only to add and remove it exits as an edit.
 * If a child enters and is converted to only add or only remove, it exits as that change.
 *
 * If an edit enters and is converted to add and remove it exits as an edit.
 * If an edit enters and is converted to only add or only remove, it exits as that change.
 * If an edit enters and exits as edits only, it exits as a single edit.
 */
export function* pushAccumulatedChanges(
  accumulatedPushes: Change[],
  output: Output,
  pusher: InputBase,
  fanOutChangeType: ChangeType,
  mergeRelationships: (existing: Change, incoming: Change) => Change,
  addEmptyRelationships: (change: Change) => Change,
): Stream<'yield'> {
  if (accumulatedPushes.length === 0) {
    // It is possible for no forks to pass along the push.
    // E.g., if no filters match in any fork.
    return;
  }

  // collapse down to a single change per type
  const candidatesToPush = new Map<ChangeType, Change>();
  for (const change of accumulatedPushes) {
    if (
      fanOutChangeType === ChangeType.CHILD &&
      change[ChangeIndex.TYPE] !== ChangeType.CHILD
    ) {
      assert(
        candidatesToPush.has(change[ChangeIndex.TYPE]) === false,
        () =>
          `Fan-in:child expected at most one ${change[ChangeIndex.TYPE]} when fan-out is of type child`,
      );
    }

    const existing = candidatesToPush.get(change[ChangeIndex.TYPE]);
    let mergedChange = change;
    if (existing) {
      // merge in relationships
      mergedChange = mergeRelationships(existing, change);
    }
    candidatesToPush.set(change[ChangeIndex.TYPE], mergedChange);
  }

  accumulatedPushes.length = 0;

  const types = [...candidatesToPush.keys()];
  /**
   * Based on the received `fanOutChangeType` only certain output types are valid.
   *
   * - remove must result in all removes
   * - add must result in all adds
   * - edit must result in add or removes or edits
   * - child must result in a single add or single remove or many child changes
   *    - Single add or remove because the relationship will be unique to one exist check within the fan-out,fan-in sub-graph
   *    - Many child changes because other operators may preserve the child change
   */
  switch (fanOutChangeType) {
    case ChangeType.REMOVE:
      assert(
        types.length === 1 && types[0] === ChangeType.REMOVE,
        'Fan-in:remove expected all removes',
      );
      yield* output.push(
        addEmptyRelationships(must(candidatesToPush.get(ChangeType.REMOVE))),
        pusher,
      );
      return;
    case ChangeType.ADD:
      assert(
        types.length === 1 && types[0] === ChangeType.ADD,
        'Fan-in:add expected all adds',
      );
      yield* output.push(
        addEmptyRelationships(must(candidatesToPush.get(ChangeType.ADD))),
        pusher,
      );
      return;
    case ChangeType.EDIT: {
      assert(
        types.every(
          type =>
            type === ChangeType.ADD ||
            type === ChangeType.REMOVE ||
            type === ChangeType.EDIT,
        ),
        'Fan-in:edit expected all adds, removes, or edits',
      );
      const addChange = candidatesToPush.get(ChangeType.ADD);
      const removeChange = candidatesToPush.get(ChangeType.REMOVE);
      let editChange = candidatesToPush.get(ChangeType.EDIT);

      // If an `edit` is present, it supersedes `add` and `remove`
      // as it semantically represents both.
      if (editChange) {
        if (addChange) {
          editChange = mergeRelationships(editChange, addChange);
        }
        if (removeChange) {
          editChange = mergeRelationships(editChange, removeChange);
        }
        yield* output.push(addEmptyRelationships(editChange), pusher);
        return;
      }

      // If `edit` didn't make it through but both `add` and `remove` did,
      // convert back to an edit.
      //
      // When can this happen?
      //
      //  EDIT old: a=1, new: a=2
      //            |
      //          FanOut
      //          /    \
      //         a=1   a=2
      //          |     |
      //        remove  add
      //          \     /
      //           FanIn
      //
      // The left filter converts the edit into a remove.
      // The right filter converts the edit into an add.
      if (addChange && removeChange) {
        yield* output.push(
          addEmptyRelationships(
            makeEditChange(
              addChange[ChangeIndex.NODE],
              removeChange[ChangeIndex.NODE],
            ),
          ),
          pusher,
        );
        return;
      }

      yield* output.push(
        addEmptyRelationships(must(addChange ?? removeChange)),
        pusher,
      );
      return;
    }
    case ChangeType.CHILD: {
      assert(
        types.every(
          type =>
            type === ChangeType.ADD || // exists can change child to add or remove
            type === ChangeType.REMOVE || // exists can change child to add or remove
            type === ChangeType.CHILD, // other operators may preserve the child change
        ),
        'Fan-in:child expected all adds, removes, or children',
      );
      assert(
        types.length <= 2,
        'Fan-in:child expected at most 2 types on a child change from fan-out',
      );

      // If any branch preserved the original child change, that takes precedence over all other changes.
      const childChange = candidatesToPush.get(ChangeType.CHILD);
      if (childChange) {
        yield* output.push(childChange, pusher);
        return;
      }

      const addChange = candidatesToPush.get(ChangeType.ADD);
      const removeChange = candidatesToPush.get(ChangeType.REMOVE);

      assert(
        addChange === undefined || removeChange === undefined,
        'Fan-in:child expected either add or remove, not both',
      );

      yield* output.push(
        addEmptyRelationships(must(addChange ?? removeChange)),
        pusher,
      );
      return;
    }
    default:
      fanOutChangeType satisfies never;
  }
}

/**
 * Puts relationships from `right` into `left` if they don't already exist in `left`.
 */
export function mergeRelationships(left: Change, right: Change): Change {
  // change types will always match
  // unless we have an edit on the left
  // then the right could be edit, add, or remove
  if (left[ChangeIndex.TYPE] === right[ChangeIndex.TYPE]) {
    switch (left[ChangeIndex.TYPE]) {
      case ChangeType.ADD: {
        return makeAddChange({
          row: left[ChangeIndex.NODE].row,
          relationships: {
            ...right[ChangeIndex.NODE].relationships,
            ...left[ChangeIndex.NODE].relationships,
          },
        });
      }
      case ChangeType.REMOVE: {
        return makeRemoveChange({
          row: left[ChangeIndex.NODE].row,
          relationships: {
            ...right[ChangeIndex.NODE].relationships,
            ...left[ChangeIndex.NODE].relationships,
          },
        });
      }
      case ChangeType.EDIT: {
        assert(
          right[ChangeIndex.TYPE] === ChangeType.EDIT,
          () =>
            `mergeRelationships: when left.type is edit and types match, right.type must be edit, got ${right[ChangeIndex.TYPE]}`,
        );
        // merge edits into a single edit
        return makeEditChange(
          {
            row: left[ChangeIndex.NODE].row,
            relationships: {
              ...right[ChangeIndex.NODE].relationships,
              ...left[ChangeIndex.NODE].relationships,
            },
          },
          {
            row: left[ChangeIndex.OLD_NODE].row,
            relationships: {
              ...right[ChangeIndex.OLD_NODE].relationships,
              ...left[ChangeIndex.OLD_NODE].relationships,
            },
          },
        );
      }
      case ChangeType.CHILD: {
        // Multiple branches may preserve the same child change, each adding
        // different relationships. Merge the relationships, keeping the child
        // (which should be identical across branches).
        assert(
          right[ChangeIndex.TYPE] === ChangeType.CHILD,
          () =>
            `mergeRelationships: when left.type is child and types match, right.type must be child, got ${right[ChangeIndex.TYPE]}`,
        );
        return makeChildChange(
          {
            row: left[ChangeIndex.NODE].row,
            relationships: {
              ...right[ChangeIndex.NODE].relationships,
              ...left[ChangeIndex.NODE].relationships,
            },
          },
          left[ChangeIndex.CHILD_DATA],
        );
      }
    }
  }

  // left is always an edit here
  assert(
    left[ChangeIndex.TYPE] === ChangeType.EDIT,
    () =>
      `mergeRelationships: when types differ, left.type must be edit, got left.type=${left[ChangeIndex.TYPE]}, right.type=${right[ChangeIndex.TYPE]}`,
  );
  switch (right[ChangeIndex.TYPE]) {
    case ChangeType.ADD: {
      return makeEditChange(
        {
          ...left[ChangeIndex.NODE],
          relationships: {
            ...right[ChangeIndex.NODE].relationships,
            ...left[ChangeIndex.NODE].relationships,
          },
        },
        left[ChangeIndex.OLD_NODE],
      );
    }
    case ChangeType.REMOVE: {
      return makeEditChange(left[ChangeIndex.NODE], {
        ...left[ChangeIndex.OLD_NODE],
        relationships: {
          ...right[ChangeIndex.NODE].relationships,
          ...left[ChangeIndex.OLD_NODE].relationships,
        },
      });
    }
  }

  unreachable();
}

export function makeAddEmptyRelationships(
  schema: SourceSchema,
): (change: Change) => Change {
  return (change: Change): Change => {
    if (Object.keys(schema.relationships).length === 0) {
      return change;
    }

    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD: {
        const relationships = {...change[ChangeIndex.NODE].relationships};
        mergeEmpty(relationships, Object.keys(schema.relationships));
        return makeAddChange({
          row: change[ChangeIndex.NODE].row,
          relationships,
        });
      }
      case ChangeType.REMOVE: {
        const relationships = {...change[ChangeIndex.NODE].relationships};
        mergeEmpty(relationships, Object.keys(schema.relationships));
        return makeRemoveChange({
          row: change[ChangeIndex.NODE].row,
          relationships,
        });
      }
      case ChangeType.EDIT: {
        const nodeRelationships = {...change[ChangeIndex.NODE].relationships};
        const oldNodeRelationships = {
          ...change[ChangeIndex.OLD_NODE].relationships,
        };
        mergeEmpty(nodeRelationships, Object.keys(schema.relationships));
        mergeEmpty(oldNodeRelationships, Object.keys(schema.relationships));
        return makeEditChange(
          {row: change[ChangeIndex.NODE].row, relationships: nodeRelationships},
          {
            row: change[ChangeIndex.OLD_NODE].row,
            relationships: oldNodeRelationships,
          },
        );
      }
      case ChangeType.CHILD:
        return change; // children only have relationships along the path to the change
    }
  };
}

/**
 * For each relationship in `schema` that does not exist
 * in `relationships`, add it with an empty stream.
 *
 * This modifies the `relationships` object in place.
 */
export function mergeEmpty(
  relationships: Record<string, () => Stream<Node | 'yield'>>,
  relationshipNames: string[],
) {
  for (const relName of relationshipNames) {
    if (relationships[relName] === undefined) {
      relationships[relName] = () => emptyArray;
    }
  }
}
