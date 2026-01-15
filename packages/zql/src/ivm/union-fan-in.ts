import {assert} from '../../../shared/src/asserts.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import type {Change} from './change.ts';
import type {Constraint} from './constraint.ts';
import type {Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
} from './operator.ts';
import {
  makeAddEmptyRelationships,
  mergeRelationships,
  pushAccumulatedChanges,
} from './push-accumulated.ts';
import type {SourceSchema} from './schema.ts';
import {first, type Stream} from './stream.ts';
import type {UnionFanOut} from './union-fan-out.ts';

export class UnionFanIn implements Operator {
  readonly #inputs: readonly Input[];
  readonly #schema: SourceSchema;
  #fanOutPushStarted: boolean = false;
  #output: Output = throwOutput;
  #accumulatedPushes: Change[] = [];

  constructor(fanOut: UnionFanOut, inputs: Input[]) {
    this.#inputs = inputs;
    const fanOutSchema = fanOut.getSchema();
    fanOut.setFanIn(this);

    const schema: Writable<SourceSchema> = {
      tableName: fanOutSchema.tableName,
      columns: fanOutSchema.columns,
      primaryKey: fanOutSchema.primaryKey,
      relationships: {
        ...fanOutSchema.relationships,
      },
      isHidden: fanOutSchema.isHidden,
      system: fanOutSchema.system,
      compareRows: fanOutSchema.compareRows,
      sort: fanOutSchema.sort,
    };

    // now go through inputs and merge relationships
    const relationshipsFromBranches: Set<string> = new Set();
    for (const input of inputs) {
      const inputSchema = input.getSchema();
      assert(
        schema.tableName === inputSchema.tableName,
        `Table name mismatch in union fan-in: ${schema.tableName} !== ${inputSchema.tableName}`,
      );
      assert(
        schema.primaryKey === inputSchema.primaryKey,
        `Primary key mismatch in union fan-in`,
      );
      assert(
        schema.system === inputSchema.system,
        `System mismatch in union fan-in: ${schema.system} !== ${inputSchema.system}`,
      );
      assert(
        schema.compareRows === inputSchema.compareRows,
        `compareRows mismatch in union fan-in`,
      );
      assert(schema.sort === inputSchema.sort, `Sort mismatch in union fan-in`);

      for (const [relName, relSchema] of Object.entries(
        inputSchema.relationships,
      )) {
        if (relName in fanOutSchema.relationships) {
          continue;
        }

        // All branches will have unique relationship names except for relationships
        // that come in from `fanOut`.
        assert(
          !relationshipsFromBranches.has(relName),
          `Relationship ${relName} exists in multiple upstream inputs to union fan-in`,
        );
        schema.relationships[relName] = relSchema;
        relationshipsFromBranches.add(relName);
      }

      input.setOutput(this);
    }

    this.#schema = schema;
    this.#inputs = inputs;
  }

  destroy(): void {
    for (const input of this.#inputs) {
      input.destroy();
    }
  }

  fetch(req: FetchRequest): Stream<Node | 'yield'> {
    const iterables = this.#inputs.map(input => input.fetch(req));
    return mergeFetches(iterables, (l, r) =>
      this.#schema.compareRows(l.row, r.row),
    );
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *push(change: Change, pusher: InputBase): Stream<'yield'> {
    if (!this.#fanOutPushStarted) {
      yield* this.#pushInternalChange(change, pusher);
    } else {
      this.#accumulatedPushes.push(change);
    }
  }

  /**
   * An internal change means that a change was received inside the fan-out/fan-in sub-graph.
   *
   * These changes always come from children of a flip-join as no other push generating operators
   * currently exist between union-fan-in and union-fan-out. All other pushes
   * enter into union-fan-out before reaching union-fan-in.
   *
   * - normal joins for `exists` come before `union-fan-out`
   * - joins for `related` come after `union-fan-out`
   * - take comes after `union-fan-out`
   *
   * The algorithm for deciding whether or not to forward a push that came from inside the ufo/ufi sub-graph:
   * 1. If the change is a `child` change we can forward it. This is because all child branches in the ufo/ufi sub-graph are unique.
   * 2. If the change is `add` we can forward it iff no `fetches` for the row return any results.
   *    If another branch has it, the add was already emitted in the past.
   * 3. If the change is `remove` we can forward it iff no `fetches` for the row return any results.
   *    If no other branches have the change, the remove can be sent as the value is no longer present.
   *    If other branches have it, the last branch the processes the remove will send the remove.
   * 4. Edits will always come through as child changes as flip join will flip them into children.
   *    An edit that would result in a remove or add will have been split into an add/remove pair rather than being an edit.
   */
  *#pushInternalChange(change: Change, pusher: InputBase): Stream<'yield'> {
    if (change.type === 'child') {
      yield* this.#output.push(change, this);
      return;
    }

    assert(
      change.type === 'add' || change.type === 'remove',
      () =>
        `UnionFanIn: expected add or remove change type, got ${change.type}`,
    );

    let hadMatch = false;
    for (const input of this.#inputs) {
      if (input === pusher) {
        hadMatch = true;
        continue;
      }

      const constraint: Writable<Constraint> = {};
      for (const key of this.#schema.primaryKey) {
        constraint[key] = change.node.row[key];
      }
      const fetchResult = input.fetch({
        constraint,
      });

      if (first(fetchResult) !== undefined) {
        // Another branch has the row, so the add/remove is not needed.
        return;
      }
    }

    assert(hadMatch, 'Pusher was not one of the inputs to union-fan-in!');

    // No other branches have the row, so we can push the change.
    yield* this.#output.push(change, this);
  }

  fanOutStartedPushing() {
    assert(
      this.#fanOutPushStarted === false,
      'UnionFanIn: fanOutStartedPushing called while already pushing',
    );
    this.#fanOutPushStarted = true;
  }

  *fanOutDonePushing(fanOutChangeType: Change['type']): Stream<'yield'> {
    assert(
      this.#fanOutPushStarted,
      'UnionFanIn: fanOutDonePushing called without fanOutStartedPushing',
    );
    this.#fanOutPushStarted = false;
    if (this.#inputs.length === 0) {
      return;
    }

    if (this.#accumulatedPushes.length === 0) {
      // It is possible for no forks to pass along the push.
      // E.g., if no filters match in any fork.
      return;
    }

    yield* pushAccumulatedChanges(
      this.#accumulatedPushes,
      this.#output,
      this,
      fanOutChangeType,
      mergeRelationships,
      makeAddEmptyRelationships(this.#schema),
    );
  }

  setOutput(output: Output): void {
    this.#output = output;
  }
}

export function* mergeFetches(
  fetches: Iterable<Node | 'yield'>[],
  comparator: (l: Node, r: Node) => number,
): IterableIterator<Node | 'yield'> {
  const iterators = fetches.map(i => i[Symbol.iterator]());
  let threw = false;
  try {
    const current: (Node | null)[] = [];
    let lastNodeYielded: Node | undefined;
    for (let i = 0; i < iterators.length; i++) {
      const iter = iterators[i];
      let result = iter.next();
      // yield yields when initializing
      while (!result.done && result.value === 'yield') {
        yield result.value;
        result = iter.next();
      }
      current[i] = result.done ? null : (result.value as Node);
    }
    while (current.some(c => c !== null)) {
      const min = current.reduce(
        (acc: [Node, number] | undefined, c, i): [Node, number] | undefined => {
          if (c === null) {
            return acc;
          }
          if (acc === undefined || comparator(c, acc[0]) < 0) {
            return [c, i];
          }
          return acc;
        },
        undefined,
      );

      assert(min !== undefined, 'min is undefined');
      const [minNode, minIndex] = min;
      const iter = iterators[minIndex];
      let result = iter.next();
      while (!result.done && result.value === 'yield') {
        yield result.value;
        result = iter.next();
      }
      current[minIndex] = result.done ? null : (result.value as Node);
      if (
        lastNodeYielded !== undefined &&
        comparator(lastNodeYielded, minNode) === 0
      ) {
        continue;
      }
      lastNodeYielded = minNode;
      yield minNode;
    }
  } catch (e) {
    threw = true;
    for (const iter of iterators) {
      try {
        iter.throw?.(e);
      } catch (_cleanupError) {
        // error in the iter.throw cleanup,
        // catch so other iterators are cleaned up
      }
    }
    throw e;
  } finally {
    if (!threw) {
      for (const iter of iterators) {
        try {
          iter.return?.();
        } catch (_cleanupError) {
          // error in the iter.return cleanup,
          // catch so other iterators are cleaned up
        }
      }
    }
  }
}
