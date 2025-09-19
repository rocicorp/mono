import {assert} from '../../../shared/src/asserts.ts';
import {mergeIterables} from '../../../shared/src/iterables.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Operator,
  type Output,
} from './operator.ts';
import {
  makeAddEmptyRelationships,
  mergeRelationships,
  pushAccumulatedChanges,
} from './push-accumulated.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';
import type {UnionFanOut} from './union-fan-out.ts';

export class UnionFanIn implements Operator {
  readonly #inputs: readonly Operator[];
  readonly #schema: SourceSchema;
  #fanOutPushStarted: boolean = false;
  #output: Output = throwOutput;
  #accumulatedPushes: Change[] = [];

  constructor(fanOut: UnionFanOut, inputs: Operator[]) {
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
    for (const input of inputs) {
      const inputSchema = input.getSchema();
      assert(
        schema.tableName === inputSchema.tableName,
        `Table name mismatch in union fan-in`,
      );
      assert(
        schema.primaryKey === inputSchema.primaryKey,
        `Primary key mismatch in union fan-in`,
      );
      assert(
        schema.system === inputSchema.system,
        `System mismatch in union fan-in`,
      );
      assert(
        schema.compareRows === inputSchema.compareRows,
        `compareRows mismatch in union fan-in`,
      );
      assert(schema.sort === inputSchema.sort, `Sort mismatch in union fan-in`);

      for (const [relName, relSchema] of Object.entries(
        inputSchema.relationships,
      )) {
        // This is not possible because only `exists` joins will be input to `union-fan-in`.
        // and `exists` joins all get unique names.
        assert(
          schema.relationships[relName] === undefined,
          `Relationship ${relName} exists in multiple upstream inputs to union fan-in`,
        );
        schema.relationships[relName] = relSchema;
      }
    }

    this.#schema = schema;
    this.#inputs = inputs;
  }

  cleanup(_req: FetchRequest): Stream<Node> {
    // Cleanup is going away. Not implemented.
    return [];
  }

  destroy(): void {
    for (const input of this.#inputs) {
      input.destroy();
    }
  }

  fetch(req: FetchRequest): Stream<Node> {
    const iterables = this.#inputs.map(input => input.fetch(req));
    return mergeIterables(
      iterables,
      (l, r) => this.#schema.compareRows(l.row, r.row),
      true,
    );
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  push(change: Change): void {
    if (!this.#fanOutPushStarted) {
      // if fan out has not started pushing but we receive a push, then it
      // must be from an input to flip join. This input is the child of the output
      // of flip join.
      assert(
        change.type === 'child',
        'Only child changes allowed to come from internal nodes in union structures',
      );
      this.#output.push(change);
    } else {
      this.#accumulatedPushes.push(change);
    }
  }

  fanOutStartedPushing() {
    assert(this.#fanOutPushStarted === false);
    this.#fanOutPushStarted = true;
  }

  fanOutDonePushing(fanOutChangeType: Change['type']) {
    assert(this.#fanOutPushStarted);
    this.#fanOutPushStarted = false;
    if (this.#inputs.length === 0) {
      return;
    }

    if (this.#accumulatedPushes.length === 0) {
      // It is possible for no forks to pass along the push.
      // E.g., if no filters match in any fork.
      return;
    }

    pushAccumulatedChanges(
      this.#accumulatedPushes,
      this.#output,
      fanOutChangeType,
      mergeRelationships,
      makeAddEmptyRelationships(this.#schema),
    );
  }

  setOutput(output: Output): void {
    this.#output = output;
  }
}
