import type {
  FetchRequest,
  HydrateRequest,
  Input,
  Operator,
  Output,
} from './operator.js';
import type {Change} from './change.js';
import type {Row} from './data.js';
import {assert} from 'shared/src/asserts.js';
import {Schema} from './schema.js';

/**
 * Snitch is an Operator that records all messages it receives. Useful for
 * debugging.
 */
export class Snitch implements Operator {
  readonly #input: Input;
  readonly #name: string;
  readonly log: SnitchMessage[];

  #output: Output | undefined;

  constructor(input: Input, name: string, log: SnitchMessage[] = []) {
    this.#input = input;
    this.#name = name;
    this.log = log;
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  getSchema(_: Output): Schema {
    return this.#input.getSchema(this);
  }

  hydrate(req: HydrateRequest, _source: Output) {
    assert(this.#output);
    this.log.push([this.#name, 'hydrate', req]);
    // Currently we don't record the `source` or the return value of hydrate().
    // If that was ever needed, we'd need to clone the stream.
    return this.#input.hydrate(req, this);
  }

  fetch(req: FetchRequest, _source: Output) {
    assert(this.#output);
    this.log.push([this.#name, 'fetch', req]);
    return this.#input.fetch(req, this);
  }

  dehydrate(req: HydrateRequest, _source: Output) {
    assert(this.#output);
    this.log.push([this.#name, 'dehydrate', req]);
    return this.#input.dehydrate(req, this);
  }

  push(change: Change, _: Input) {
    this.log.push([this.#name, 'push', toChangeRecord(change)]);
    this.#output?.push(change, this);
  }
}

function toChangeRecord(change: Change): ChangeRecord {
  if (change.type === 'add') {
    return {type: 'add', row: change.node.row};
  }
  if (change.type === 'remove') {
    return {type: 'remove', row: change.node.row};
  }
  return {
    type: 'child',
    row: change.row,
    child: toChangeRecord(change.child.change),
  };
}

export type SnitchMessage =
  | HydrateMessage
  | FetchMessage
  | DehydrateMessage
  | PushMessage;

export type HydrateMessage = [string, 'hydrate', HydrateRequest];
export type FetchMessage = [string, 'fetch', FetchRequest];
export type DehydrateMessage = [string, 'dehydrate', HydrateRequest];
export type PushMessage = [string, 'push', ChangeRecord];

export type ChangeRecord =
  | AddChangeRecord
  | RemoveChangeRecord
  | ChildChangeRecord;

export type AddChangeRecord = {
  type: 'add';
  row: Row;
  // We don't currently capture the relationships. If we did, we'd need a
  // stream that cloned them lazily.
};

export type RemoveChangeRecord = {
  type: 'remove';
  row: Row;
};

export type ChildChangeRecord = {
  type: 'child';
  row: Row;
  child: ChangeRecord;
};
