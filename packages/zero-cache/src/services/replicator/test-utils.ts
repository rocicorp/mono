import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {ColumnSpec, IndexSpec, TableSpec} from '../../db/specs.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {RowKey, RowValue} from '../../types/row-key.ts';
import type {
  ColumnAdd,
  ColumnDrop,
  ColumnUpdate,
  DataChange,
  IndexCreate,
  IndexDrop,
  MessageBegin,
  MessageCommit,
  MessageDelete,
  MessageInsert,
  MessageRelation,
  MessageRollback,
  MessageTruncate,
  MessageUpdate,
  TableCreate,
  TableDrop,
  TableRename,
} from '../change-source/protocol/current/data.ts';
import {ChangeProcessor} from './change-processor.ts';

export interface FakeReplicator {
  processTransaction(finalWatermark: string, ...msgs: DataChange[]): void;
}

export function fakeReplicator(lc: LogContext, db: Database): FakeReplicator {
  const messageProcessor = createChangeProcessor(db);
  return {
    processTransaction: (watermark, ...msgs) => {
      messageProcessor.processMessage(lc, [
        'begin',
        {tag: 'begin'},
        {commitWatermark: watermark},
      ]);
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

export function createChangeProcessor(
  db: Database,
  failures: (lc: LogContext, err: unknown) => void = (_, err) => {
    throw err;
  },
): ChangeProcessor {
  return new ChangeProcessor(new StatementRunner(db), 'serving', failures);
}

export class ReplicationMessages<
  TablesAndKeys extends Record<string, string | string[]>,
> {
  readonly #tables = new Map<string, MessageRelation>();

  constructor(
    tablesAndKeys: TablesAndKeys,
    schema = 'public',
    replicaIdentity: 'default' | 'full' = 'default',
  ) {
    for (const [table, k] of Object.entries(tablesAndKeys)) {
      const keys = typeof k === 'string' ? [k] : [...k];
      const relation = {
        tag: 'relation',
        schema,
        name: table,
        rowKey: {
          type: replicaIdentity,
          columns: keys,
        },
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

  createTable(spec: TableSpec): TableCreate {
    return {
      tag: 'create-table',
      spec,
      metadata: {
        rowKey: {
          columns: must(spec.primaryKey),
          type: 'default',
        },
      },
    };
  }

  renameTable<TableName extends string & keyof TablesAndKeys>(
    from: TableName,
    to: string,
  ): TableRename {
    return {
      tag: 'rename-table',
      old: {schema: 'public', name: from},
      new: {schema: 'public', name: to},
    };
  }

  addColumn<TableName extends string & keyof TablesAndKeys>(
    table: TableName,
    column: string,
    spec: ColumnSpec,
  ): ColumnAdd {
    return {
      tag: 'add-column',
      table: {schema: 'public', name: table},
      column: {name: column, spec},
      tableMetadata: {
        rowKey: {
          columns: ['not-mocked-for-the-test'],
        },
      },
    };
  }

  updateColumn<TableName extends string & keyof TablesAndKeys>(
    table: TableName,
    oldCol: {name: string; spec: ColumnSpec},
    newCol: {name: string; spec: ColumnSpec},
  ): ColumnUpdate {
    return {
      tag: 'update-column',
      table: {schema: 'public', name: table},
      old: oldCol,
      new: newCol,
    };
  }

  dropColumn<TableName extends string & keyof TablesAndKeys>(
    table: TableName,
    column: string,
  ): ColumnDrop {
    return {
      tag: 'drop-column',
      table: {schema: 'public', name: table},
      column,
    };
  }

  createIndex(spec: IndexSpec): IndexCreate {
    return {tag: 'create-index', spec};
  }

  dropTable(name: string, schema = 'public'): TableDrop {
    return {tag: 'drop-table', id: {schema, name}};
  }

  dropIndex(name: string, schema = 'public'): IndexDrop {
    return {tag: 'drop-index', id: {schema, name}};
  }

  commit(extra?: object): MessageCommit {
    return {tag: 'commit', ...extra};
  }

  rollback(): MessageRollback {
    return {tag: 'rollback'};
  }
}
