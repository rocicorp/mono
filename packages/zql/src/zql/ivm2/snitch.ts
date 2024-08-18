import type {
  FetchRequest,
  HydrateRequest,
  Input,
  Operator,
  Output,
  Schema,
} from './operator.js';
import type {Change} from './change.js';
import type {Row} from './data.js';
import {assert} from 'shared/src/asserts.js';

/**
 * Snitch is an Operator that records all messages it receives. Useful for
 * debugging.
 */
export class Snitch implements Operator {
  readonly #input: Input;
  readonly messages: unknown[] = [];

  #output: Output | undefined;

  constructor(input: Input) {
    this.#input = input;
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  get schema(): Schema {
    return this.#input.schema;
  }

  hydrate(req: HydrateRequest, _source: Output) {
    assert(this.#output);
    this.messages.push(['hydrate', req]);
    // Currently we don't record the `source` or the return value of hydrate().
    // If that was ever needed, we'd need to clone the stream.
    return this.#input.hydrate(req, this);
  }

  fetch(req: FetchRequest, _source: Output) {
    assert(this.#output);
    this.messages.push(['fetch', req]);
    return this.#input.fetch(req, this);
  }

  dehydrate(req: HydrateRequest, _source: Output) {
    assert(this.#output);
    this.messages.push(['dehydrate', req]);
    return this.#input.dehydrate(req, this);
  }

  push(change: Change, _: Input) {
    this.messages.push(['push', toChangeRecord(change)]);
  }

  reset() {
    this.messages.length = 0;
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

export type Messages =
  | HydrateMessage
  | FetchMessage
  | DehydrateMessage
  | PushMessage;

export type HydrateMessage = ['hydrate', HydrateRequest];
export type FetchMessage = ['fetch', FetchRequest];
export type DehydrateMessage = ['dehydrate', HydrateRequest];
export type PushMessage = ['push', ChangeRecord];

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
