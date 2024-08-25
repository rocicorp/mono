import type {FetchRequest, Input, Operator, Output} from './operator.js';
import type {Change} from './change.js';
import type {Row} from './data.js';
import {assert} from 'shared/src/asserts.js';
import type {Schema} from './schema.js';

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

    this.#input.setOutput(this);
  }

  destroy(): void {
    this.#input.destroy();
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  getSchema(): Schema {
    return this.#input.getSchema();
  }

  fetch(req: FetchRequest) {
    assert(this.#output);
    this.log.push([this.#name, 'fetch', req]);
    return this.#input.fetch(req);
  }

  cleanup(req: FetchRequest) {
    assert(this.#output);
    this.log.push([this.#name, 'cleanup', req]);
    return this.#input.cleanup(req);
  }

  push(change: Change) {
    this.log.push([this.#name, 'push', toChangeRecord(change)]);
    this.#output?.push(change);
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

export type SnitchMessage = FetchMessage | CleanupMessage | PushMessage;

export type FetchMessage = [string, 'fetch', FetchRequest];
export type CleanupMessage = [string, 'cleanup', FetchRequest];
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
