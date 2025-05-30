import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {Row} from './data.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import type {
  FilterInput,
  FilterOperator,
  FilterOutput,
} from './filter-operators.ts';
import {
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

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
    input.setOutput(this);
  }

  destroy(): void {
    this.#input.destroy();
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  #log(message: SnitchMessage) {
    if (!this.#logTypes.includes(message[1])) {
      return;
    }
    this.log.push(message);
  }

  fetch(req: FetchRequest): Stream<Node> {
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
      return {
        type: change.type,
        row: change.node.row,
        oldRow: change.oldNode.row,
      };
    case 'child':
      return {
        type: 'child',
        row: change.node.row,
        child: toChangeRecord(change.child.change),
      };
    default:
      unreachable(change);
  }
}

/**
 * Snitch is an Operator that records all messages it receives. Useful for
 * debugging.
 */
export class FilterSnitch implements FilterOperator {
  readonly #input: FilterInput;
  readonly #name: string;
  readonly #logTypes: LogType[];
  readonly log: SnitchMessage[];

  #output: FilterOutput | undefined;

  constructor(
    input: FilterInput,
    name: string,
    log: SnitchMessage[] = [],
    logTypes: LogType[] = ['filter', 'push', 'cleanup'],
  ) {
    this.#input = input;
    this.#name = name;
    this.log = log;
    this.#logTypes = logTypes;
    input.setFilterOutput(this);
  }

  setFilterOutput(output: FilterOutput): void {
    this.#output = output;
  }

  filter(node: Node, cleanup: boolean): boolean {
    this.#log([this.#name, 'filter', node.row, cleanup ? 'cleanup' : 'fetch']);
    assert(this.#output);
    return this.#output.filter(node, cleanup);
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  #log(message: SnitchMessage) {
    if (!this.#logTypes.includes(message[1])) {
      return;
    }
    this.log.push(message);
  }

  push(change: Change) {
    this.#log([this.#name, 'push', toChangeRecord(change)]);
    this.#output?.push(change);
  }
}

export type SnitchMessage =
  | FetchMessage
  | FetchCountMessage
  | CleanupMessage
  | PushMessage
  | FilterMessage;

export type FetchCountMessage = [string, 'fetchCount', FetchRequest, number];
export type FetchMessage = [string, 'fetch', FetchRequest];
export type CleanupMessage = [string, 'cleanup', FetchRequest];
export type PushMessage = [string, 'push', ChangeRecord];
export type FilterMessage = [string, 'filter', Row, 'fetch' | 'cleanup'];

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

export type LogType = 'fetch' | 'push' | 'cleanup' | 'fetchCount' | 'filter';
