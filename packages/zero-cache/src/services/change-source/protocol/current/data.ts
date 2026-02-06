/**
 * Data plane messages encapsulate changes that are sent by ChangeSources,
 * forwarded / fanned out to subscribers by the ChangeStreamerService, and
 * stored in the Change DB for catchup of old subscribers.
 */

import {
  jsonValueSchema,
  type JSONObject,
} from '../../../../../../shared/src/bigint-json.ts';
import {must} from '../../../../../../shared/src/must.ts';
import * as v from '../../../../../../shared/src/valita.ts';
import {columnSpec, indexSpec, tableSpec} from '../../../../db/specs.ts';
import type {Satisfies} from '../../../../types/satisfies.ts';

export const beginSchema = v.object({
  tag: v.literal('begin'),
  // The format of values of "json"-typed columns (e.g. "JSON" and "JSONB").
  // - 'p' is for parsed JSON, which may include JSON values or JSON objects.
  //   These values are parsed and stringified at every process boundary
  //   between the change-source and the replica.
  // - 's' is for stringified JSON. These values skip the parsing and
  //   stringification, and are directly ferried to the replica as a JSON
  //   string. For JSON values this improves performance by 20~25% in the
  //   change-streamer and 25~30% in the replicator.
  //
  // If absent, the format is assumed to be 'p' (parsed JSON objects/values).
  json: v.literalUnion('p', 's').optional(),
});

export const commitSchema = v.object({
  tag: v.literal('commit'),
});

export const rollbackSchema = v.object({
  tag: v.literal('rollback'),
});

const rowKeySchema = v.object({
  // The columns used to identify a row in insert, update, and delete changes.
  columns: v.array(v.string()),

  // An optional qualifier identifying how the key is chosen. Currently this
  // is postgres-specific, describing the REPLICA IDENTITY, for which replica
  // identity 'full' (FULL) is handled differently; the replicator handles
  // these tables by extracting a row key from the full row based on the
  // table's PRIMARY KEY or UNIQUE INDEX.
  type: v.literalUnion('default', 'nothing', 'full', 'index').optional(),
});

export const relationSchema = v
  .object({
    schema: v.string(),
    name: v.string(),

    // This will become required.
    rowKey: rowKeySchema.optional(),

    /** Deprecated: set the rowKey.columns instead. */
    keyColumns: v.array(v.string()).optional(),
    /** Deprecated: set the rowKey.columns instead. */
    replicaIdentity: v
      .literalUnion('default', 'nothing', 'full', 'index')
      .optional(),
  })
  .map(rel => {
    const {rowKey, ...rest} = rel;
    if (rowKey) {
      return {...rest, rowKey};
    }
    return {
      ...rest,
      rowKey: {
        columns: must(rel.keyColumns),
        type: rel.replicaIdentity,
      },
    };
  });

// The eventual fate of relationSchema
export const newRelationSchema = v.object({
  schema: v.string(),
  name: v.string(),

  rowKey: rowKeySchema,
});

// TableMetadata contains table-related configuration that does not affect the
// actual data in the table, but rather how the table's change messages are
// handled.
//
// Changes to the metadata are sent in an `table-update-metadata` change.
export const tableMetadataSchema = v.object({
  // The rowKey is the same object sent in the `relation` message of
  // `insert`, `update`, and `delete` messages.
  rowKey: rowKeySchema,
});

export type TableMetadata = v.Infer<typeof tableMetadataSchema>;

export const rowSchema = v.record(jsonValueSchema);

export const insertSchema = v.object({
  tag: v.literal('insert'),
  relation: relationSchema,
  new: rowSchema,
});

export const updateSchema = v.object({
  tag: v.literal('update'),
  relation: relationSchema,
  // `key` is present if the update changed the key of the row, or if the
  // table's replicaIdentity === 'full'
  key: rowSchema.nullable(),
  // `new` is the full row (and not just the updated columns). This is
  // necessary for "catchup" replication scenarios such as adding tables
  // to a publication, or resharding.
  new: rowSchema,
});

export const deleteSchema = v.object({
  tag: v.literal('delete'),
  relation: relationSchema,
  // key is the full row if replicaIdentity === 'full'
  key: rowSchema,
});

export const truncateSchema = v.object({
  tag: v.literal('truncate'),
  relations: v.array(relationSchema),
});

export const identifierSchema = v.object({
  schema: v.string(),
  name: v.string(),
});

export type Identifier = v.Infer<typeof identifierSchema>;

// A BackfillID is an upstream specific stable identifier for a column
// that needs backfilling. This id is used to ensure that the schema, table,
// and column names of a requested backfill still match the original
// underlying upstream ID.
//
// The change-streamer stores these IDs as opaque values while a column is
// being backfilled, and initiates new change-source streams with the IDs
// in order to restart backfills that did not complete in previous sessions.
export const backfillIDSchema = v.record(v.union(v.number(), v.string()));

export type BackfillID = v.Infer<typeof backfillIDSchema>;

export const createTableSchema = v.object({
  tag: v.literal('create-table'),
  spec: tableSpec,

  // This must be set by change source implementations that support
  // table/column backfill.
  //
  // TODO: to simplify the protocol, see if we can make this required
  metadata: tableMetadataSchema.optional(),

  // Indicate that columns of the table require backfilling. These columns
  // should be created on the replica but not yet synced the clients.
  //
  // ## State Persistence
  //
  // To obviate the need for change-source implementations to persist state
  // related to backfill progress, the change-source only tracks backfills
  // **for the current session**. In the event that the session is interrupted
  // before columns have been fully backfilled, it is the responsibility of the
  // change-streamer to send {@link BackfillRequest}s when it reconnects.
  //
  // This means that the change-streamer must track and persist:
  // * the backfill IDs of the columns requiring backfilling
  // * the most current table metadata of the associated table(s)
  //
  // The change-streamer then uses this information to send backfill requests
  // when it reconnects.
  backfill: v.record(backfillIDSchema).optional(),
});

export const renameTableSchema = v.object({
  tag: v.literal('rename-table'),
  old: identifierSchema,
  new: identifierSchema,
});

export const updateTableMetadataSchema = v.object({
  tag: v.literal('update-table-metadata'),
  table: identifierSchema,
  old: tableMetadataSchema,
  new: tableMetadataSchema,
});

const columnSchema = v.object({
  name: v.string(),
  spec: columnSpec,
});

export const addColumnSchema = v.object({
  tag: v.literal('add-column'),
  table: identifierSchema,
  column: columnSchema,

  // This must be set by change source implementations that support
  // table/column backfill.
  //
  // TODO: to simplify the protocol, see if we can make this required
  tableMetadata: tableMetadataSchema.optional(),

  // See documentation for the `backfill` field of the `create-table` change.
  backfill: backfillIDSchema.optional(),
});

export const updateColumnSchema = v.object({
  tag: v.literal('update-column'),
  table: identifierSchema,
  old: columnSchema,
  new: columnSchema,
});

export const dropColumnSchema = v.object({
  tag: v.literal('drop-column'),
  table: identifierSchema,
  column: v.string(),
});

export const dropTableSchema = v.object({
  tag: v.literal('drop-table'),
  id: identifierSchema,
});

export const createIndexSchema = v.object({
  tag: v.literal('create-index'),
  spec: indexSpec,
});

export const dropIndexSchema = v.object({
  tag: v.literal('drop-index'),
  id: identifierSchema,
});

// A batch of rows from a single table containing column values
// to be backfilled.
export const backfillSchema = v.object({
  tag: v.literal('backfill'),

  relation: newRelationSchema,

  // The columns to be backfilled. `rowKey` columns are automatically excluded,
  // which means that this field may be empty.
  columns: v.array(v.string()),

  // The watermark at which the backfill data was queried. Note that this
  // generally will be different from the commit watermarks of the main change
  // stream, and in particular, the commit watermark of the backfill change's
  // enclosing transaction.
  watermark: v.string(),

  // A batch of row values, each row consisting of the `rowKey`
  // values, followed by the `column` values, in the same order in which
  // the column names appear in their respective fields, e.g.
  //
  // ```
  // [
  //   [...rowKeyValues, ...columnValues],  // row 1
  //   [...rowKeyValues, ...columnValues],  // row 2
  // ]
  // ```
  rowValues: v.array(v.array(jsonValueSchema)),
});

// Indicates that the backfill for the specified columns have
// been successfully backfilled and can be published to clients.
export const backfillCompletedSchema = v.object({
  tag: v.literal('backfill-completed'),

  relation: newRelationSchema,

  // The columns to be backfilled. `rowKey` columns are automatically excluded,
  // which means that this field may be empty.
  columns: v.array(v.string()),
});

export type MessageBegin = v.Infer<typeof beginSchema>;
export type MessageCommit = v.Infer<typeof commitSchema>;
export type MessageRollback = v.Infer<typeof rollbackSchema>;

export type MessageRelation = v.Infer<typeof relationSchema>;
export type MessageInsert = v.Infer<typeof insertSchema>;
export type MessageUpdate = v.Infer<typeof updateSchema>;
export type MessageDelete = v.Infer<typeof deleteSchema>;
export type MessageTruncate = v.Infer<typeof truncateSchema>;

export type MessageBackfill = v.Infer<typeof backfillSchema>;

export type TableCreate = v.Infer<typeof createTableSchema>;
export type TableRename = v.Infer<typeof renameTableSchema>;
export type TableUpdateMetadata = v.Infer<typeof updateTableMetadataSchema>;
export type ColumnAdd = v.Infer<typeof addColumnSchema>;
export type ColumnUpdate = v.Infer<typeof updateColumnSchema>;
export type ColumnDrop = v.Infer<typeof dropColumnSchema>;
export type TableDrop = v.Infer<typeof dropTableSchema>;
export type IndexCreate = v.Infer<typeof createIndexSchema>;
export type IndexDrop = v.Infer<typeof dropIndexSchema>;
export type BackfillCompleted = v.Infer<typeof backfillCompletedSchema>;

const schemaChanges = [
  createTableSchema,
  renameTableSchema,
  updateTableMetadataSchema,
  addColumnSchema,
  updateColumnSchema,
  dropColumnSchema,
  dropTableSchema,
  createIndexSchema,
  dropIndexSchema,
  backfillCompletedSchema,
] as const;

// Note: keep in sync or the tag tests will fail
const schemaChangeTags = [
  'create-table',
  'rename-table',
  'update-table-metadata',
  'add-column',
  'update-column',
  'drop-column',
  'drop-table',
  'create-index',
  'drop-index',
  'backfill-completed',
] as const;

export const schemaChangeSchema = v.union(...schemaChanges);

const schemaChangeTagsSchema = v.literalUnion(...schemaChangeTags);

export type SchemaChange = Satisfies<
  JSONObject,
  v.Infer<typeof schemaChangeSchema>
>;

export type SchemaChangeTag = v.Infer<typeof schemaChangeTagsSchema>;

export const dataChangeSchema = v.union(
  insertSchema,
  updateSchema,
  deleteSchema,
  truncateSchema,
  backfillSchema,
);

// Note: keep in sync or the tag tests will fail
const dataChangeTags = [
  'insert',
  'update',
  'delete',
  'truncate',
  'backfill',
] as const;

const dataChangeTagsSchema = v.literalUnion(...dataChangeTags);

export type DataChange = Satisfies<
  JSONObject, // guarantees serialization over IPC or network
  v.Infer<typeof dataChangeSchema>
>;

export type DataChangeTag = v.Infer<typeof dataChangeTagsSchema>;

export type DataOrSchemaChange = DataChange | SchemaChange;

export type Change =
  | MessageBegin
  | DataOrSchemaChange
  | MessageCommit
  | MessageRollback;

export type ChangeTag = Change['tag'];

const schemaChangeTagSet = new Set<string>(schemaChangeTags);

export function isSchemaChange(change: Change): change is SchemaChange {
  return schemaChangeTagSet.has(change.tag);
}

const dataChangeTagSet = new Set<string>(dataChangeTags);

export function isDataChange(change: Change): change is DataChange {
  return dataChangeTagSet.has(change.tag);
}
