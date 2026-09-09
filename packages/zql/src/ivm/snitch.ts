import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import {
  type FilterInput,
  type FilterOperator,
  type FilterOutput,
} from './filter-operators.ts';
import {
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {PullStream, Stream} from './stream.ts';

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

  fetch(req: FetchRequest): PullStream<Node | 'yield'> {
    this.#log([this.#name, 'fetch', req]);
    const input = this.#input.fetch(req);
    let count = 0;
    let done = false;
    const finish = () => {
      if (!done) {
        done = true;
        input.close();
        this.#log([this.#name, 'fetchCount', req, count]);
      }
    };
    return {
      next: (): Node | 'yield' | undefined => {
        if (done) {
          return undefined;
        }
        let node: Node | 'yield' | undefined;
        try {
          node = input.next();
        } catch (e) {
          // The generator logged the count from a `finally`, so a throw
          // mid-scan still recorded what had been read.
          finish();
          throw e;
        }
        if (node === undefined) {
          finish();
          return undefined;
        }
        if (node !== 'yield') {
          count++;
        }
        return node;
      },
      close: finish,
    };
  }

  *push(change: Change): Stream<'yield'> {
    this.#log([this.#name, 'push', toChangeRecord(change)]);
    if (this.#output) {
      yield* this.#output.push(change, this);
    }
  }
}

function toChangeRecord(change: Change): ChangeRecord {
  switch (change[ChangeIndex.TYPE]) {
    case ChangeType.ADD:
      return {type: 'add', row: change[ChangeIndex.NODE].row};
    case ChangeType.REMOVE:
      return {type: 'remove', row: change[ChangeIndex.NODE].row};
    case ChangeType.EDIT:
      return {
        type: 'edit',
        row: change[ChangeIndex.NODE].row,
        oldRow: change[ChangeIndex.OLD_NODE].row,
      };
    case ChangeType.CHILD:
      return {
        type: 'child',
        row: change[ChangeIndex.NODE].row,
        child: toChangeRecord(change[ChangeIndex.CHILD_DATA].change),
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

  /** The node whose 'filter' has been logged but not yet resolved. */
  #logged: Node | undefined;

  filterPull(node: Node): boolean | 'yield' {
    if (this.#logged !== node) {
      this.#log([this.#name, 'filter', node.row]);
      this.#logged = node;
    }
    assert(this.#output, 'Snitch: output must be set before filter is called');
    const r = this.#output.filterPull(node);
    if (r !== 'yield') {
      this.#logged = undefined;
    }
    return r;
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
