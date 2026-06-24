import {areEqual} from '../../../shared/src/arrays.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {
  makeAddChange,
  makeEditChange,
  makeRemoveChange,
  type Change,
} from './change.ts';
import type {Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';
import {mergeFetches} from './union-fan-in.ts';

/**
 * Merges the results of an OR query that has more than one good starting point.
 *
 * Query:
 *
 *   item
 *     |-- status = 'open'
 *     `-- OR EXISTS(item_tag WHERE tag = 'bug')
 *
 * Scan plan:
 *
 *   branch 0: item(status = 'open') ---------------.
 *                                                  InputUnion -> item rows
 *   branch 1: item_tag(tag = 'bug') -> item(id) ----'
 *
 * InputUnion streams those item rows in final sort order and removes duplicate
 * item ids. If the same item appears in both branches, the earlier branch is
 * the visible copy. Push has to preserve that same rule, including handoffs:
 *
 *   before push:
 *     branch 0: empty
 *     branch 1: {id: 1, value: 20}
 *     output:   {id: 1, value: 20}
 *
 *   after branch 0 adds {id: 1, value: 10}:
 *     branch 0: {id: 1, value: 10}
 *     branch 1: {id: 1, value: 20}
 *     output:   {id: 1, value: 10}
 *
 * The visible result did not gain a new primary key. The visible copy changed,
 * so downstream receives EDIT(value 20 -> 10).
 *
 * This operator intentionally accepts only plain row streams with no
 * relationships. The builder strips condition-only relationship payloads from
 * branch pipelines before they enter the union. That keeps the schema honest:
 * every node emitted by the union has the same shape that the union advertises.
 */
export class InputUnion implements Input {
  readonly #inputs: readonly Input[];
  readonly #inputIndexes: ReadonlyMap<InputBase, number>;
  readonly #schema: SourceSchema;
  #output: Output = throwOutput;

  constructor(inputs: readonly Input[]) {
    assert(inputs.length > 0, 'InputUnion requires at least one input');
    this.#inputs = inputs;
    this.#inputIndexes = new Map(inputs.map((input, index) => [input, index]));
    this.#schema = mergeInputSchemas('input union', inputs);
    for (const input of inputs) {
      input.setOutput(this);
    }
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  fetch(req: FetchRequest): Stream<Node | 'yield'> {
    const compareRows = req.reverse
      ? (left: Node, right: Node) =>
          this.#schema.compareRows(right.row, left.row)
      : (left: Node, right: Node) =>
          this.#schema.compareRows(left.row, right.row);
    return mergeFetches(
      this.#inputs.map(input => input.fetch(req)),
      compareRows,
    );
  }

  *push(change: Change, pusher: InputBase): Stream<'yield'> {
    const pusherIndex = this.#inputIndexes.get(pusher);
    assert(pusherIndex !== undefined, 'Pusher was not an input union input');

    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        yield* this.#pushAdd(change[ChangeIndex.NODE], pusher, pusherIndex);
        return;

      case ChangeType.REMOVE:
        yield* this.#pushRemove(change[ChangeIndex.NODE], pusher, pusherIndex);
        return;

      case ChangeType.EDIT: {
        const oldNode = change[ChangeIndex.OLD_NODE];
        const newNode = change[ChangeIndex.NODE];
        if (
          rowKey(oldNode.row, this.#schema.primaryKey) !==
          rowKey(newNode.row, this.#schema.primaryKey)
        ) {
          yield* this.#pushRemove(oldNode, pusher, pusherIndex);
          yield* this.#pushAdd(newNode, pusher, pusherIndex);
          return;
        }

        if (yield* this.#earlierInputHasMatch(pusherIndex, newNode)) {
          return;
        }
        yield* this.#output.push(change, this);
        return;
      }

      case ChangeType.CHILD:
        if (
          yield* this.#earlierInputHasMatch(
            pusherIndex,
            change[ChangeIndex.NODE],
          )
        ) {
          return;
        }
        yield* this.#output.push(change, this);
        return;
    }
  }

  destroy(): void {
    for (const input of this.#inputs) {
      input.destroy();
    }
  }

  *#pushAdd(
    node: Node,
    pusher: InputBase,
    pusherIndex: number,
  ): Generator<'yield'> {
    // ADD has three cases:
    //
    //   no other branch has pk       => new visible row, emit ADD
    //   later branch has pk          => visible-copy handoff, emit EDIT
    //   earlier branch has pk        => still represented earlier, emit nothing
    const match = yield* this.#firstMatchingInputExcept(pusher, node);
    if (!match) {
      yield* this.#output.push(makeAddChange(node), this);
      return;
    }
    if (pusherIndex < match.index && !rowsEqual(node.row, match.node.row)) {
      yield* this.#output.push(makeEditChange(node, match.node), this);
    }
  }

  *#pushRemove(
    node: Node,
    pusher: InputBase,
    pusherIndex: number,
  ): Generator<'yield'> {
    // REMOVE mirrors ADD:
    //
    //   no other branch has pk       => row disappeared, emit REMOVE
    //   later branch has pk          => visible-copy handoff, emit EDIT
    //   earlier branch has pk        => still represented earlier, emit nothing
    const match = yield* this.#firstMatchingInputExcept(pusher, node);
    if (!match) {
      yield* this.#output.push(makeRemoveChange(node), this);
      return;
    }
    if (pusherIndex < match.index && !rowsEqual(node.row, match.node.row)) {
      yield* this.#output.push(makeEditChange(match.node, node), this);
    }
  }

  *#firstMatchingInputExcept(
    pusher: InputBase,
    node: Node,
  ): Generator<
    'yield',
    {readonly index: number; readonly node: Node} | undefined
  > {
    // Search in input order because fetch uses input order as the tie-break for
    // duplicate primary keys. Push must discover the same visible copy.
    const constraint = keyConstraint(node.row, this.#schema.primaryKey);
    for (const [index, input] of this.#inputs.entries()) {
      if (input === pusher) {
        continue;
      }
      const matchingNode = yield* firstMatchingNode(input, constraint);
      if (matchingNode) {
        return {index, node: matchingNode};
      }
    }
    return undefined;
  }

  *#earlierInputHasMatch(
    pusherIndex: number,
    node: Node,
  ): Generator<'yield', boolean> {
    const constraint = keyConstraint(node.row, this.#schema.primaryKey);
    for (const input of this.#inputs.slice(0, pusherIndex)) {
      if (yield* inputHasMatch(input, constraint)) {
        return true;
      }
    }
    return false;
  }
}

/**
 * Keeps only parent ids that appear in every required related-table scan.
 *
 * Query:
 *
 *   item
 *     |-- EXISTS(item_tag WHERE tag = 'bug')
 *     `-- AND EXISTS(item_watch WHERE user_id = 7)
 *
 * Scan plan:
 *
 *   item_tag(tag = 'bug')   -> item_id ids {10, 20} --.
 *                                                        InputIntersection -> id {20}
 *   item_watch(user_id = 7) -> item_ref ids {20, 30} ---'
 *                                                               |
 *                                                               v
 *                                                           item(id = 20)
 *
 * The important idea is simple: do the cheap child-table lookups first, keep
 * only ids found in every branch, then fetch the parent rows. The builder only
 * creates this operator when each child branch can produce at most one row for
 * a given parent id. Different child tables may name that id differently,
 * so `inputKeys` maps every branch key back to the first branch's key. That
 * keeps "one row representing item id 20" equivalent to EXISTS semantics. If a
 * future caller wants arbitrary many rows per parent id, this operator would
 * need to track duplicate child rows instead of just ids.
 */
export class InputIntersection implements Input {
  readonly #inputs: readonly Input[];
  readonly #inputKeys: readonly CompoundKey[];
  readonly #schema: SourceSchema;
  readonly #inputIndexes: ReadonlyMap<InputBase, number>;
  #output: Output = throwOutput;

  constructor(
    inputs: readonly Input[],
    key: CompoundKey,
    inputKeys: readonly CompoundKey[] = inputs.map(() => key),
  ) {
    assert(inputs.length > 0, 'InputIntersection requires at least one input');
    assert(
      inputKeys.length === inputs.length,
      'InputIntersection requires one key per input',
    );
    assert(
      inputKeys.every(inputKey => inputKey.length === key.length),
      'InputIntersection keys must have the same width',
    );
    this.#inputs = inputs;
    this.#inputKeys = inputKeys;
    this.#schema = inputs[0].getSchema();
    this.#inputIndexes = new Map(inputs.map((input, index) => [input, index]));
    for (const [index, input] of inputs.entries()) {
      assertKeyColumnsExist(input.getSchema(), inputKeys[index]);
    }
    for (const input of inputs) {
      input.setOutput(this);
    }
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    // Fetch the other branch id sets first, then stream rows from the first
    // branch in its own order. We intentionally materialize id sets for the
    // non-representative branches, not full rows from the first branch:
    //
    //   rest branches -> Set(parent id)
    //                          |
    //                          v
    //   first branch stream -> keep ids present in every Set
    //
    // yieldedKeys is the set part of the contract: even if the first branch
    // has duplicate rows for an id, the intersection emits that id only once
    // for the parent lookup.
    const [first, ...rest] = this.#inputs;
    const matchingKeys: ReadonlySet<string>[] = [];
    for (const [restIndex, input] of rest.entries()) {
      const keys = new Set<string>();
      const inputIndex = restIndex + 1;
      for (const node of input.fetch(
        this.#fetchRequestForInput(req, inputIndex),
      )) {
        if (node === 'yield') {
          yield node;
          continue;
        }
        keys.add(this.#rowKey(inputIndex, node.row));
      }
      matchingKeys.push(keys);
    }

    const yieldedKeys = new Set<string>();
    for (const node of first.fetch(req)) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      const key = this.#rowKey(0, node.row);
      if (!yieldedKeys.has(key) && matchingKeys.every(keys => keys.has(key))) {
        yieldedKeys.add(key);
        yield node;
      }
    }
  }

  *push(change: Change, pusher: InputBase): Stream<'yield'> {
    assert(isInput(pusher), 'Expected pusher to be an input');
    const pusherIndex = this.#inputIndexes.get(pusher);
    assert(pusherIndex !== undefined, 'Pusher was not an input');

    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        yield* this.#pushAdd(change[ChangeIndex.NODE], pusher, pusherIndex);
        return;

      case ChangeType.REMOVE:
        yield* this.#pushRemove(change[ChangeIndex.NODE], pusher, pusherIndex);
        return;

      case ChangeType.EDIT: {
        const oldNode = change[ChangeIndex.OLD_NODE];
        const newNode = change[ChangeIndex.NODE];
        if (
          this.#rowKey(pusherIndex, oldNode.row) !==
          this.#rowKey(pusherIndex, newNode.row)
        ) {
          yield* this.#pushRemove(oldNode, pusher, pusherIndex);
          yield* this.#pushAdd(newNode, pusher, pusherIndex);
          return;
        }

        if (
          pusher === this.#inputs[0] &&
          (yield* this.#allOtherInputsHaveMatch(pusher, pusherIndex, newNode))
        ) {
          yield* this.#output.push(change, this);
        }
        return;
      }

      case ChangeType.CHILD:
        if (
          pusher === this.#inputs[0] &&
          (yield* this.#allOtherInputsHaveMatch(
            pusher,
            pusherIndex,
            change[ChangeIndex.NODE],
          ))
        ) {
          yield* this.#output.push(change, this);
        }
        return;
    }
  }

  destroy(): void {
    for (const input of this.#inputs) {
      input.destroy();
    }
  }

  *#pushAdd(
    node: Node,
    pusher: Input,
    pusherIndex: number,
  ): Generator<'yield'> {
    // An id enters the intersection only when every other branch has that id.
    // Non-first branches emit the first input's row, because fetch always uses
    // the first branch as the visible copy for an intersected id.
    if (!(yield* this.#allOtherInputsHaveMatch(pusher, pusherIndex, node))) {
      return;
    }
    if (pusher === this.#inputs[0]) {
      yield* this.#output.push(makeAddChange(node), this);
      return;
    }
    const visibleCopy = yield* firstMatchingNode(
      this.#inputs[0],
      this.#keyConstraintForInput(0, this.#keyValues(pusherIndex, node.row)),
    );
    if (visibleCopy) {
      yield* this.#output.push(makeAddChange(visibleCopy), this);
    }
  }

  *#pushRemove(
    node: Node,
    pusher: Input,
    pusherIndex: number,
  ): Generator<'yield'> {
    // An id leaves the intersection when this branch no longer has it and all
    // other branches still do. For the first branch, removing the current
    // visible copy is visible even if another first-branch row with the same
    // id remains, because fetch would have emitted the removed row before the
    // change.
    const values = this.#keyValues(pusherIndex, node.row);
    const constraint = this.#keyConstraintForInput(pusherIndex, values);
    if (
      pusher !== this.#inputs[0] &&
      (yield* inputHasMatch(pusher, constraint))
    ) {
      return;
    }
    if (!(yield* this.#allOtherInputsHaveMatch(pusher, pusherIndex, node))) {
      return;
    }
    if (pusher === this.#inputs[0]) {
      yield* this.#output.push(makeRemoveChange(node), this);
      return;
    }
    const visibleCopy = yield* firstMatchingNode(
      this.#inputs[0],
      this.#keyConstraintForInput(0, values),
    );
    if (visibleCopy) {
      yield* this.#output.push(makeRemoveChange(visibleCopy), this);
    }
  }

  *#allOtherInputsHaveMatch(
    pusher: InputBase,
    pusherIndex: number,
    node: Node,
  ): Generator<'yield', boolean> {
    // This tests id presence, not row equality. EXISTS only cares whether each
    // branch can produce at least one related row for the parent id.
    const values = this.#keyValues(pusherIndex, node.row);
    for (const [index, input] of this.#inputs.entries()) {
      if (input === pusher) {
        continue;
      }
      if (
        !(yield* inputHasMatch(
          input,
          this.#keyConstraintForInput(index, values),
        ))
      ) {
        return false;
      }
    }
    return true;
  }

  #fetchRequestForInput(req: FetchRequest, inputIndex: number): FetchRequest {
    if (inputIndex === 0 || !req.constraint) {
      return req;
    }

    const firstKey = this.#inputKeys[0];
    const inputKey = this.#inputKeys[inputIndex];
    const constraint: Record<string, Value> = {};
    for (const [index, key] of firstKey.entries()) {
      const value = req.constraint[key];
      if (value !== undefined) {
        constraint[inputKey[index]] = value;
      }
    }
    return Object.keys(constraint).length === 0 ? {} : {constraint};
  }

  #rowKey(inputIndex: number, row: Row): string {
    return JSON.stringify(this.#keyValues(inputIndex, row));
  }

  #keyValues(inputIndex: number, row: Row): readonly Value[] {
    return this.#inputKeys[inputIndex].map(column => row[column]);
  }

  #keyConstraintForInput(
    inputIndex: number,
    values: readonly Value[],
  ): Record<string, Value> {
    return Object.fromEntries(
      this.#inputKeys[inputIndex].map((column, index) => [
        column,
        values[index],
      ]),
    );
  }
}

function mergeInputSchemas(
  operatorName: string,
  inputs: readonly Input[],
): SourceSchema {
  // InputUnion dedupes duplicate parent rows by choosing one visible copy. It
  // cannot honestly merge relationship payloads from multiple copies, so it
  // refuses relationship-bearing inputs. Callers that use relationships as
  // private filter evidence must strip them before constructing the union.
  const schema = {
    ...firstInputSchema(operatorName, inputs),
    relationships: {},
  } satisfies Writable<SourceSchema>;

  for (const input of inputs) {
    const inputSchema = input.getSchema();
    assertCompatibleSchema(operatorName, schema, inputSchema);
    assert(
      Object.keys(inputSchema.relationships).length === 0,
      `${operatorName} requires inputs without relationships`,
    );
  }

  return schema;
}

function firstInputSchema(
  operatorName: string,
  inputs: readonly Input[],
): SourceSchema {
  const schema = inputs[0].getSchema();
  for (const input of inputs.slice(1)) {
    assertCompatibleSchema(operatorName, schema, input.getSchema());
  }
  return schema;
}

function assertKeyColumnsExist(schema: SourceSchema, key: CompoundKey): void {
  for (const column of key) {
    assert(
      schema.columns[column] !== undefined,
      `InputIntersection key column ${column} missing from ${schema.tableName}`,
    );
  }
}

function assertCompatibleSchema(
  operatorName: string,
  expected: SourceSchema,
  actual: SourceSchema,
): void {
  assert(
    expected.tableName === actual.tableName,
    `Table name mismatch in ${operatorName}: ${expected.tableName} !== ${actual.tableName}`,
  );
  assert(
    areEqual(expected.primaryKey, actual.primaryKey),
    `Primary key mismatch in ${operatorName}`,
  );
  assert(
    expected.system === actual.system,
    `System mismatch in ${operatorName}: ${expected.system} !== ${actual.system}`,
  );
  assert(
    JSON.stringify(expected.sort) === JSON.stringify(actual.sort),
    `Sort mismatch in ${operatorName}`,
  );
}

function keyConstraint(
  row: Row,
  key: readonly string[],
): Record<string, Value> {
  return Object.fromEntries(key.map(column => [column, row[column]]));
}

function rowKey(row: Row, key: readonly string[]): string {
  return JSON.stringify(key.map(column => row[column]));
}

function rowsEqual(left: Row, right: Row): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function isInput(input: InputBase): input is Input {
  return 'fetch' in input;
}

function* inputHasMatch(
  input: Input,
  constraint: Record<string, Value>,
): Generator<'yield', boolean> {
  for (const node of input.fetch({constraint})) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    return true;
  }
  return false;
}

function* firstMatchingNode(
  input: Input,
  constraint: Record<string, Value>,
): Generator<'yield', Node | undefined> {
  for (const node of input.fetch({constraint})) {
    if (node === 'yield') {
      yield node;
      continue;
    }
    return node;
  }
  return undefined;
}
