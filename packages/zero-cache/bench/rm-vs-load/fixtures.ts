import type {TableSpec} from '../../src/db/specs.ts';
import type {
  DataOrSchemaChange,
  MessageDelete,
  IndexCreate,
  MessageInsert,
  MessageRelation,
  MessageUpdate,
} from '../../src/services/change-source/protocol/current/data.ts';
import type {
  Begin,
  Commit,
  Data,
} from '../../src/services/change-source/protocol/current/downstream.ts';
import {ReplicationMessages} from '../../src/services/replicator/test-utils.ts';

type PayloadSize = 'small' | 'medium' | 'large';
type TransactionMessage = Begin | Data | Commit;

export type PayloadProfile = {
  readonly size: PayloadSize;
  readonly bytes: number;
};

const payloadBytes: Record<PayloadSize, number> = {
  small: 96,
  medium: 1024,
  large: 8192,
};

export const loadPayloadProfiles: readonly PayloadProfile[] = [
  {size: 'small', bytes: payloadBytes.small},
  {size: 'medium', bytes: payloadBytes.medium},
  {size: 'large', bytes: payloadBytes.large},
];

export const smokePayloadProfiles: readonly PayloadProfile[] = [
  {size: 'small', bytes: payloadBytes.small},
];

export type LoadScenario = {
  readonly rowsPerTx: number;
  readonly payload: PayloadProfile;
};

export type GeneratedTransaction = {
  readonly watermark: string;
  readonly changes: TransactionMessage[];
  readonly rows: number;
  readonly operationCounts: OperationCounts;
};

export type OperationCounts = {
  insert: number;
  update: number;
  delete: number;
};

export function emptyOperationCounts(): OperationCounts {
  return {insert: 0, update: 0, delete: 0};
}

const tableName = 'bench_rows';
const messages = new ReplicationMessages({[tableName]: 'id'});
export const benchRelation: MessageRelation = {
  schema: 'public',
  name: tableName,
  rowKey: {type: 'default', columns: ['id']},
};

export const benchTableSpec: TableSpec = {
  schema: 'public',
  name: tableName,
  primaryKey: ['id'],
  columns: {
    id: {pos: 1, dataType: 'text', notNull: true},
    tx: {pos: 2, dataType: 'int8', notNull: true},
    seq: {pos: 3, dataType: 'int8', notNull: true},
    bucket: {pos: 4, dataType: 'int4', notNull: true},
    payload: {pos: 5, dataType: 'text', notNull: true},
  },
};

export const benchPrimaryIndex: IndexCreate = messages.createIndex({
  schema: 'public',
  name: `${tableName}_pk`,
  tableName,
  unique: true,
  columns: {id: 'ASC'},
});

const payloadPattern =
  '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
const payloadCache = new Map<number, string>();

function payloadFor(bytes: number): string {
  let payload = payloadCache.get(bytes);
  if (payload !== undefined) {
    return payload;
  }
  payload = payloadPattern
    .repeat(Math.ceil(bytes / payloadPattern.length))
    .slice(0, bytes);
  payloadCache.set(bytes, payload);
  return payload;
}

export function watermarkFor(tx: number): string {
  return tx.toString(36).padStart(12, '0');
}

export function makeInsert(
  tx: number,
  seq: number,
  bytes: number,
  id = rowID(tx, seq),
): MessageInsert {
  return {
    tag: 'insert',
    relation: benchRelation,
    new: makeRow(id, tx, seq, bytes),
  };
}

export function makeUpdate(
  id: string,
  tx: number,
  seq: number,
  bytes: number,
): MessageUpdate {
  return messages.update(tableName, makeRow(id, tx, seq, bytes));
}

export function makeDelete(id: string): MessageDelete {
  return messages.delete(tableName, {id});
}

function rowID(tx: number, seq: number): string {
  return `${tx.toString(36)}-${seq.toString(36)}`;
}

function makeRow(id: string, tx: number, seq: number, bytes: number) {
  return {
    id,
    tx,
    seq,
    bucket: seq % 32,
    payload: payloadFor(bytes),
  };
}

export function makeTransaction(
  tx: number,
  rowsPerTx: number,
  payload: PayloadProfile,
): GeneratedTransaction {
  const watermark = watermarkFor(tx);
  const changes: TransactionMessage[] = [
    ['begin', messages.begin(), {commitWatermark: watermark}],
  ];

  for (let i = 0; i < rowsPerTx; i++) {
    changes.push(['data', makeInsert(tx, i, payload.bytes)]);
  }

  changes.push(['commit', messages.commit(), {watermark}]);

  return {
    watermark,
    changes,
    rows: rowsPerTx,
    operationCounts: {insert: rowsPerTx, update: 0, delete: 0},
  };
}

export function makeSchemaChanges(): DataOrSchemaChange[] {
  return [messages.createTable(benchTableSpec), benchPrimaryIndex];
}

export function makeTransactions(
  count: number,
  rowsPerTx: number,
  payload: PayloadProfile,
  startTx = 1,
): GeneratedTransaction[] {
  return Array.from({length: count}, (_, i) =>
    makeTransaction(startTx + i, rowsPerTx, payload),
  );
}
