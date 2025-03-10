import {assert} from '../../../shared/src/asserts.ts';
import {mergeIterables} from '../../../shared/src/iterables.ts';
import {must} from '../../../shared/src/must.ts';
import type {Change, EditChange} from './change.ts';
import type {Node} from './data.ts';
import type {FanOut} from './fan-out.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

/**
 * The FanIn operator merges multiple streams into one.
 * It eliminates duplicates and must be paired with a fan-out operator
 * somewhere upstream of the fan-in.
 *
 *  issue
 *    |
 * fan-out
 * /      \
 * a      b
 *  \    /
 * fan-in
 *   |
 */
export class FanIn implements Operator {
  readonly #inputs: readonly Input[];
  readonly #schema: SourceSchema;
  #output: Output = throwOutput;
  #accumulatedPushes: Change[];

  constructor(fanOut: FanOut, inputs: Input[]) {
    this.#inputs = inputs;
    this.#schema = fanOut.getSchema();
    for (const input of inputs) {
      input.setOutput(this);
      assert(this.#schema === input.getSchema(), `Schema mismatch in fan-in`);
    }
    this.#accumulatedPushes = [];
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  destroy(): void {
    for (const input of this.#inputs) {
      input.destroy();
    }
  }

  getSchema() {
    return this.#schema;
  }

  fetch(req: FetchRequest): Stream<Node> {
    return this.#fetchOrCleanup(input => input.fetch(req));
  }

  cleanup(req: FetchRequest): Stream<Node> {
    return this.#fetchOrCleanup(input => input.cleanup(req));
  }

  *#fetchOrCleanup(streamProvider: (input: Input) => Stream<Node>) {
    const iterables = this.#inputs.map(input => streamProvider(input));
    yield* mergeIterables(
      iterables,
      (l, r) => must(this.#schema).compareRows(l.row, r.row),
      true,
    );
  }

  push(change: Change) {
    this.#accumulatedPushes.push(change);
  }

  fanOutDonePushingToAllBranches(fanOutChangeType: Change['type']) {
    if (this.#inputs.length === 0) {
      assert(
        this.#inputs.length === 0,
        'If there are no inputs then fan-in should not receive any pushes.',
      );
      return;
    }

    if (this.#accumulatedPushes.length === 0) {
      // It is possible for no forks to pass along the push.
      // E.g., if no filters match in any fork.
      // This can happen for all change types: add, remove, edit, child.
      // In `edit`, both the old and new rows can be filtered out.
      // In `child`, the parent row can be filtered out.
      // In `add` and `remove`, the row itself can be filtered out.
      return;
    }

    // We collect changes for each type to determine what to push.
    // We do not need all changes for each type as the changes for most types are expected
    // to be identical.
    //
    // Exists is a subtle case, however. A `child` change coming into `fanOut` can be
    // converted into a `remove` or `add` where the `relationships` have been modified by `exists`.
    //
    //    | type:child
    //  fanOut
    //    | type:child
    //   exists
    //    | type:remove
    //   fanIn
    //
    // In this case, the changes must be merged to preserve the `relationship` editing done by `exists`.
    //
    //
    // We only collect 1 change for each type. It would be incorrect to output the same change type
    // many times. See the comment at the bottom of this file.
    //
    // >> What happens if we ever allow `join` in between `fan-in`, `fan-out`? <<
    //    That would be nonsense. It would mean the shape of the query result is
    //    different depending on which branches evaluated to true.
    //
    const candidatesToPush = new Map<Change['type'], Change>();
    for (const change of this.#accumulatedPushes) {
      const existing = candidatesToPush.get(change.type);
      if (existing !== undefined) {
        if (existing.node === change.node) {
          // 1. We have recorded a change for this type
          // 2. The nodes are identical
          // Can skip this change.
          continue;
        }

        assert(
          existing.node.row === change.node.row,
          'Expected identical rows',
        );

        // This is the nuanced case of `exists` modifying the relationships in a `child` change.
        assert(
          fanOutChangeType === 'child' &&
            // NOTE: you can get `edit` here for nested ors
            (change.type === 'add' ||
              change.type === 'remove' ||
              change.type === 'edit'),
          'Expected child change',
        );
        const replace: Change =
          change.type === 'edit'
            ? {
                type: 'edit',
                node: {
                  row: change.node.row,
                  relationships: {
                    ...existing.node.relationships,
                    ...change.node.relationships,
                  },
                },
                oldNode: {
                  row: change.oldNode.row,
                  relationships: {
                    ...(existing as EditChange).oldNode.relationships,
                    ...change.oldNode.relationships,
                  },
                },
              }
            : {
                type: change.type,
                node: {
                  row: change.node.row,
                  relationships: {
                    ...existing.node.relationships,
                    ...change.node.relationships,
                  },
                },
              };
        candidatesToPush.set(change.type, replace);
        continue;
      }

      candidatesToPush.set(change.type, change);
    }

    this.#accumulatedPushes = [];

    const types = [...candidatesToPush.keys()];
    /**
     * Based on the received `fanOutChangeType` only certain output types are valid.
     *
     * - remove must result in all removes
     * - add must result in all adds
     * - edit must result in add or removes or edits
     * - child...
     */
    switch (fanOutChangeType) {
      case 'remove':
        assert(
          types.length === 1 && types[0] === 'remove',
          'Expected all removes',
        );
        this.#output.push(must(candidatesToPush.get('remove')));
        return;
      case 'add':
        assert(types.length === 1 && types[0] === 'add', 'Expected all adds');
        this.#output.push(must(candidatesToPush.get('add')));
        return;
      case 'edit': {
        assert(
          types.every(
            type => type === 'add' || type === 'remove' || type === 'edit',
          ),
          'Expected all adds, removes, or edits',
        );
        const addChange = candidatesToPush.get('add');
        const removeChange = candidatesToPush.get('remove');
        const editChange = candidatesToPush.get('edit');

        // If an `edit` is present, it supersedes `add` and `remove`
        // as it semantically represents both.
        if (editChange) {
          this.#output.push(editChange);
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
          // convert back to edit
          this.#output.push({
            type: 'edit',
            node: addChange.node,
            oldNode: removeChange.node,
          } as const);
          return;
        }

        this.#output.push(must(addChange ?? removeChange));
        return;
      }
      case 'child': {
        // complex case due to exists
        // one branch could add and another remove or vice-versa.
        assert(
          types.every(
            type =>
              type === 'add' || // exists can change child to add or remove
              type === 'remove' || // exists can change child to add or remove
              type === 'child' || // other operators may preserve the child change
              type === 'edit', // edit can show up in the case of nested `or` expressions.
          ),
          'Expected all adds, removes, or children',
        );

        // If any branch preserved the original child change, that takes precedence over all other changes.
        // This is due to the nature of `or`. Explained at the bottom of this file.
        const childChange = candidatesToPush.get('child');
        if (childChange) {
          this.#output.push(childChange);
          return;
        }

        const editChange = candidatesToPush.get('edit');
        const addChange = candidatesToPush.get('add');
        const removeChange = candidatesToPush.get('remove');

        if (editChange) {
          this.#output.push(editChange);
          return;
        }

        if (addChange && removeChange) {
          // convert to an edit
          // hm.... do we want to do this?
          // If we do not, this results in a "split push" -- child splitting to many changes.
          // If we do, this means an `edit` can show up in this case if `fan-in` and `fan-out` are nested
          // within an outer `fan-in` and `fan-out`.
          this.#output.push({
            type: 'edit',
            node: addChange.node,
            oldNode: removeChange.node,
          } as const);
          return;
        }

        this.#output.push(must(addChange ?? removeChange));
        return;
      }
      default:
        fanOutChangeType satisfies never;
    }
  }
}

/**
 * Why does `or`:
 * 1. Only push a single change for a given type?
 * 2. Prefer `child` over `add` and `remove`?
 * 3. Prefer `edit` over `add` and `remove`?
 *
 * (1) - because `or` duplicates a change on fan-out:
 *
 *    x > 1 OR x > 2 OR x > 3
 *
 *           FanOut
 *         /   |    \
 *      x>1   x>2   x>3
 *       \     |     /
 *        \    |    /
 *           FanIn
 *
 * All branches could evaluate to true, resulting in 3 `add` or `remove` changes for
 * the same change.
 *
 * And edit can be split into add and remove by the above graph.
 * A single `remove` and single `add` should be output by the `fan-in` (or those two changes rolled into edit).
 *
 * (2) - similar reasoning to the above.
 * If a branch outputs `child` while other branches may output `add` or `remove`, the `child`
 * change logically subsumes them. <--- can you be more specific here?
 *
 * (3) - similar reasoning to the above.
 */
