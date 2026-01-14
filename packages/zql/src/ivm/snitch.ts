import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
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
    logTypes: LogType[] = ['fetch', 'push'],
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

  fetch(req: FetchRequest): Stream<Node | 'yield'> {
    this.#log([this.#name, 'fetch', req]);
    return this.fetchGenerator(req);
  }

  *fetchGenerator(req: FetchRequest): Stream<Node | 'yield'> {
    let count = 0;
    try {
      for (const node of this.#input.fetch(req)) {
        if (node === 'yield') {
          yield node;
          continue;
        }
        count++;
        yield node;
      }
    } finally {
      this.#log([this.#name, 'fetchCount', req, count]);
    }
  }

  *push(change: Change): Stream<'yield'> {
    this.#log([this.#name, 'push', toChangeRecord(change)]);
    if (this.#output) {
      yield* this.#output.push(change, this);
    }
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
    logTypes: LogType[] = ['filter', 'push'],
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

  beginFilter(): void {
    this.#output?.beginFilter();
  }

  endFilter(): void {
    this.#output?.endFilter();
  }

  *filter(node: Node): Generator<'yield', boolean> {
    this.#log([this.#name, 'filter', node.row]);
    assert(this.#output, 'Snitch: output must be set before filter is called');
    return yield* this.#output.filter(node);
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

  *push(change: Change): Stream<'yield'> {
    this.#log([this.#name, 'push', toChangeRecord(change)]);
    if (this.#output) {
      yield* this.#output.push(change, this);
    }
  }
}

export type SnitchMessage =
  | FetchMessage
  | FetchCountMessage
  | PushMessage
  | FilterMessage;

export type FetchCountMessage = [string, 'fetchCount', FetchRequest, number];
export type FetchMessage = [string, 'fetch', FetchRequest];
export type PushMessage = [string, 'push', ChangeRecord];
export type FilterMessage = [string, 'filter', Row];

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

export type LogType = 'fetch' | 'push' | 'fetchCount' | 'filter';
