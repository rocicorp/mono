import {assert, unreachable} from 'shared/src/asserts.js';
import type {Change} from './change.js';
import type {Node, Row} from './data.js';
import type {FetchRequest, Input, Operator, Output} from './operator.js';
import type {Schema} from './schema.js';
import type {Stream} from './stream.js';

/**
 * Snitch is an Operator that records all messages it receives. Useful for
 * debugging.
 */
export class Snitch implements Operator {
  readonly #input: Input;
  readonly #name: string;
  readonly #logTypes: LogType[];
  readonly log: SnitchMessage[];

  #output: Output | undefined;

  constructor(
    input: Input,
    name: string,
    log: SnitchMessage[] = [],
    logTypes: LogType[] = ['fetch', 'push', 'cleanup'],
  ) {
    this.#input = input;
    this.#name = name;
    this.log = log;
    this.#logTypes = logTypes;
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

  #log(message: SnitchMessage) {
    if (!this.#logTypes.includes(message[1])) {
      return;
    }
    this.log.push(message);
  }

  fetch(req: FetchRequest): Stream<Node> {
    assert(this.#output);
    this.#log([this.#name, 'fetch', req]);
    return this.fetchGenerator(req);
  }

  *fetchGenerator(req: FetchRequest): Stream<Node> {
    let count = 0;
    try {
      for (const node of this.#input.fetch(req)) {
        count++;
        yield node;
      }
    } finally {
      this.#log([this.#name, 'fetchCount', req, count]);
    }
  }

  cleanup(req: FetchRequest) {
    assert(this.#output);
    this.#log([this.#name, 'cleanup', req]);
    return this.#input.cleanup(req);
  }

  push(change: Change) {
    this.#log([this.#name, 'push', toChangeRecord(change)]);
    this.#output?.push(change);
  }
}

function toChangeRecord(change: Change): ChangeRecord {
  switch (change.type) {
    case 'add':
    case 'remove':
      return {type: change.type, row: change.node.row};
    case 'edit':
      return change;
    case 'child':
      return {
        type: 'child',
        row: change.row,
        child: toChangeRecord(change.child.change),
      };
    default:
      unreachable(change);
  }
}

export type SnitchMessage =
  | FetchMessage
  | FetchCountMessage
  | CleanupMessage
  | PushMessage;

export type FetchCountMessage = [string, 'fetchCount', FetchRequest, number];
export type FetchMessage = [string, 'fetch', FetchRequest];
export type CleanupMessage = [string, 'cleanup', FetchRequest];
export type PushMessage = [string, 'push', ChangeRecord];

export type ChangeRecord =
  | AddChangeRecord
  | RemoveChangeRecord
  | ChildChangeRecord
  | EditChangeRecord;

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

export type EditChangeRecord = {
  type: 'edit';
  row: Row;
  oldRow: Row;
};

export type LogType = 'fetch' | 'push' | 'cleanup' | 'fetchCount';
