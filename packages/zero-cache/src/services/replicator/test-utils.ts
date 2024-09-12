import {LogContext} from '@rocicorp/logger';
import {Pgoutput} from 'pg-logical-replication';
import {assert} from 'shared/src/asserts.js';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {
  versionFromLexi,
  versionToLexi,
} from 'zero-cache/src/types/lexi-version.js';
import {RowKey, RowValue} from 'zero-cache/src/types/row-key.js';
import {h32} from 'zero-cache/src/types/xxhash.js';
import {Database} from 'zqlite/src/db.js';
import {
  Change,
  MessageBegin,
  MessageCommit,
} from '../change-streamer/schema/change.js';
import {MessageProcessor} from './incremental-sync.js';

const NOOP = () => {};

export interface FakeReplicator {
  process(...msgs: Change[]): void;

  processTransaction(
    finalWatermark: string,
    ...msgs: (
      | Pgoutput.MessageInsert
      | Pgoutput.MessageDelete
      | Pgoutput.MessageUpdate
    )[]
  ): void;
}

export function fakeReplicator(lc: LogContext, db: Database): FakeReplicator {
  const messageProcessor = createMessageProcessor(db);
  let watermark = 123;
  return {
    process: (...msgs) => {
      for (const msg of msgs) {
        messageProcessor.processMessage(lc, versionToLexi(watermark++), msg);
      }
    },

    processTransaction: (finalWatermark, ...msgs) => {
      let watermark = Number(versionFromLexi(finalWatermark)) - msgs.length - 1;
      messageProcessor.processMessage(lc, versionToLexi(watermark++), {
        tag: 'begin',
      });
      for (const msg of msgs) {
        messageProcessor.processMessage(lc, versionToLexi(watermark++), msg);
      }
      messageProcessor.processMessage(lc, finalWatermark, {
        tag: 'commit',
      });
    },
  };
}

export function createMessageProcessor(
  db: Database,
  ack: (lsn: string) => void = NOOP,
  versions: () => void = NOOP,
  failures: (lc: LogContext, err: unknown) => void = NOOP,
): MessageProcessor {
  return new MessageProcessor(new StatementRunner(db), ack, versions, failures);
}

export class ReplicationMessages<
  TablesAndKeys extends Record<string, string | string[]>,
> {
  readonly #tables = new Map<string, Pgoutput.MessageRelation>();

  constructor(tablesAndKeys: TablesAndKeys) {
    for (const [table, k] of Object.entries(tablesAndKeys)) {
      const keys = typeof k === 'string' ? [k] : [...k];
      const relation = {
        tag: 'relation',
        relationOid: h32(table), // deterministic for snapshot-friendliness
        schema: 'public',
        name: table,
        replicaIdentity: 'default',
        columns: keys.map(name => ({
          flags: 1,
          name,
          typeOid: 23,
          typeMod: -1,
          typeSchema: null,
          typeName: null,
          parser: () => {},
        })),
        keyColumns: keys,
      } as const;
      this.#tables.set(table, relation);
    }
  }

  #relationOrFail(table: string): Pgoutput.MessageRelation {
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
  ): Pgoutput.MessageInsert {
    return {tag: 'insert', relation: this.#relationOrFail(table), new: row};
  }

  update<TableName extends string & keyof TablesAndKeys>(
    table: TableName,
    row: RowValue,
    oldKey?: RowKey,
  ): Pgoutput.MessageUpdate {
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
  ): Pgoutput.MessageDelete {
    return {
      tag: 'delete',
      relation: this.#relationOrFail(table),
      key,
      old: null,
    };
  }

  truncate<TableName extends string & keyof TablesAndKeys>(
    table: TableName,
    ...moreTables: TableName[]
  ): Pgoutput.MessageTruncate {
    const tables = [table, ...moreTables];
    return {
      tag: 'truncate',
      relations: tables.map(t => this.#relationOrFail(t)),
      cascade: false,
      restartIdentity: false,
    };
  }

  commit(extra?: object): MessageCommit {
    return {tag: 'commit', ...extra};
  }
}
