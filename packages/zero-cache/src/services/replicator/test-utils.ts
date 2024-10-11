import {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.js';
import {Database} from '../../../../zqlite/src/db.js';
import {StatementRunner} from '../../db/statements.js';
import type {RowKey, RowValue} from '../../types/row-key.js';
import type {
  DataChange,
  MessageBegin,
  MessageCommit,
  MessageDelete,
  MessageInsert,
  MessageRelation,
  MessageTruncate,
  MessageUpdate,
} from '../change-streamer/schema/change.js';
import {MessageProcessor} from './incremental-sync.js';

const NOOP = () => {};

export interface FakeReplicator {
  processTransaction(finalWatermark: string, ...msgs: DataChange[]): void;
}

export function fakeReplicator(lc: LogContext, db: Database): FakeReplicator {
  const messageProcessor = createMessageProcessor(db);
  return {
    processTransaction: (watermark, ...msgs) => {
      messageProcessor.processMessage(lc, ['begin', {tag: 'begin'}]);
      for (const msg of msgs) {
        messageProcessor.processMessage(lc, ['data', msg]);
      }
      messageProcessor.processMessage(lc, [
        'commit',
        {tag: 'commit'},
        {watermark},
      ]);
    },
  };
}

export function createMessageProcessor(
  db: Database,
  ack: (lsn: string) => void = NOOP,
  failures: (lc: LogContext, err: unknown) => void = NOOP,
): MessageProcessor {
  return new MessageProcessor(
    new StatementRunner(db),
    'IMMEDIATE',
    ack,
    failures,
  );
}

export class ReplicationMessages<
  TablesAndKeys extends Record<string, string | string[]>,
> {
  readonly #tables = new Map<string, MessageRelation>();

  constructor(tablesAndKeys: TablesAndKeys, schema = 'public') {
    for (const [table, k] of Object.entries(tablesAndKeys)) {
      const keys = typeof k === 'string' ? [k] : [...k];
      const relation = {
        tag: 'relation',
        schema,
        name: table,
        replicaIdentity: 'default',
        keyColumns: keys,
      } as const;
      this.#tables.set(table, relation);
    }
  }

  #relationOrFail(table: string): MessageRelation {
    const relation = this.#tables.get(table);
    assert(relation); // Type parameters should guarantee this.
    return relation;
  }

  begin(): MessageBegin {
    return {tag: 'begin'};
  }

  insert<TableName extends string & keyof TablesAndKeys>(
    table: TableName,
    row: RowValue,
  ): MessageInsert {
    return {tag: 'insert', relation: this.#relationOrFail(table), new: row};
  }

  update<TableName extends string & keyof TablesAndKeys>(
    table: TableName,
    row: RowValue,
    oldKey?: RowKey,
  ): MessageUpdate {
    return {
      tag: 'update',
      relation: this.#relationOrFail(table),
      new: row,
      key: oldKey ?? null,
      old: null,
    };
  }

  delete<TableName extends string & keyof TablesAndKeys>(
    table: TableName,
    key: RowKey,
  ): MessageDelete {
    return {
      tag: 'delete',
      relation: this.#relationOrFail(table),
      key,
    };
  }

  truncate<TableName extends string & keyof TablesAndKeys>(
    table: TableName,
    ...moreTables: TableName[]
  ): MessageTruncate {
    const tables = [table, ...moreTables];
    return {
      tag: 'truncate',
      relations: tables.map(t => this.#relationOrFail(t)),
    };
  }

  commit(extra?: object): MessageCommit {
    return {tag: 'commit', ...extra};
  }
}
