import {assert} from '../../../shared/src/asserts.ts';
import {mergeIterables} from '../../../shared/src/iterables.ts';
import {must} from '../../../shared/src/must.ts';
import type {Change} from './change.ts';
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
        this.#accumulatedPushes.length === 0,
        'If there are no inputs then fan-in should not receive any pushes.',
      );
      return;
    }

    if (this.#accumulatedPushes.length === 0) {
      // It is possible for no forks to pass along the push.
      // E.g., if no filters match in any fork.
      return;
    }

    // collapse down to a single change per type
    const candidatesToPush = new Map<Change['type'], Change>();
    for (const change of this.#accumulatedPushes) {
      if (fanOutChangeType === 'child' && change.type !== 'child') {
        assert(
          candidatesToPush.has(change.type) === false,
          () =>
            `Fan-in:child expected at most one ${change.type} when fan-out is of type child`,
        );
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
     * - child must result in a single add or single remove or many child changes
     */
    switch (fanOutChangeType) {
      case 'remove':
        assert(
          types.length === 1 && types[0] === 'remove',
          'Fan-in:remove expected all removes',
        );
        this.#output.push(must(candidatesToPush.get('remove')));
        return;
      case 'add':
        assert(
          types.length === 1 && types[0] === 'add',
          'Fan-in:add expected all adds',
        );
        this.#output.push(must(candidatesToPush.get('add')));
        return;
      case 'edit': {
        assert(
          types.every(
            type => type === 'add' || type === 'remove' || type === 'edit',
          ),
          'Fan-in:edit expected all adds, removes, or edits',
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
        assert(
          types.every(
            type =>
              type === 'add' || // exists can change child to add or remove
              type === 'remove' || // exists can change child to add or remove
              type === 'child', // other operators may preserve the child change
          ),
          'Fan-in:child expected all adds, removes, or children',
        );
        assert(
          types.length <= 2,
          'Fan-in:child expected at most 2 types on a child change from fan-out',
        );

        // If any branch preserved the original child change, that takes precedence over all other changes.
        const childChange = candidatesToPush.get('child');
        if (childChange) {
          this.#output.push(childChange);
          return;
        }

        const addChange = candidatesToPush.get('add');
        const removeChange = candidatesToPush.get('remove');

        assert(
          addChange === undefined || removeChange === undefined,
          'Fan-in:child expected either add or remove, not both',
        );

        this.#output.push(must(addChange ?? removeChange));
        return;
      }
      default:
        fanOutChangeType satisfies never;
    }
  }
}
