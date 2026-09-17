import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {
  DataOrSchemaChange,
  MessageBackfill,
} from '../change-source/protocol/current/data.ts';
import {ChangeProcessor} from './change-processor.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {ReplicationMessages} from './test-utils.ts';

export const lc = createSilentLogContext();
export const messages = new ReplicationMessages({items: 'id'});
export const relation = {
  schema: 'public',
  name: 'items',
  rowKey: {columns: ['id']},
};

/** Real replica application, shared by the RM and serving-replica repros. */
export function replica(mode: 'backup' | 'serving', ids: number[] = [1]) {
  const db = new Database(lc, ':memory:');
  initReplicationState(db, ['zero_data'], '00');
  const processor = new ChangeProcessor(
    new StatementRunner(db),
    mode,
    (_, err) => {
      throw err;
    },
  );
  function transaction(watermark: string, ...changes: DataOrSchemaChange[]) {
    processor.processMessage(lc, [
      'begin',
      {tag: 'begin'},
      {commitWatermark: watermark},
    ]);
    for (const change of changes) {
      processor.processMessage(lc, ['data', change]);
    }
    processor.processMessage(lc, ['commit', {tag: 'commit'}, {watermark}]);
  }
  transaction(
    '01',
    messages.createTable({
      schema: 'public',
      name: 'items',
      primaryKey: ['id'],
      columns: {
        id: {pos: 1, dataType: 'int4'},
        label: {pos: 2, dataType: 'text'},
      },
    }),
    messages.createIndex({
      schema: 'public',
      name: 'items_pkey',
      tableName: 'items',
      unique: true,
      columns: {id: 'ASC'},
    }),
    ...ids.map(id => messages.insert('items', {id, label: `row ${id}`})),
  );
  transaction(
    '02',
    messages.addColumn(
      'items',
      'body',
      {pos: 3, dataType: 'text'},
      {
        tableMetadata: {rowKey: {id: {attNum: 1}}},
        backfill: {attNum: 3},
      },
    ),
  );
  return {
    db,
    processor,
    transaction,
    rows: () =>
      db.prepare('SELECT id, label, body FROM items ORDER BY id').all(),
  };
}

export function batch(ids: number[], watermark = '03'): MessageBackfill {
  return {
    tag: 'backfill',
    relation,
    columns: ['body'],
    watermark,
    rowValues: ids.map(id => [id, `body ${id}`]),
  };
}
