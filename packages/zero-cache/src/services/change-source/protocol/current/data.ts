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
import {jsonObjectSchema} from './json.ts';
import {schemaChangeTags} from './schema-change-tags.ts';

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

  // Directs the change-streamer to skip the ACK for the corresponding commit.
  skipAck: v.boolean().optional(),
});

export const commitSchema = v.object({
  tag: v.literal('commit'),

  // Millisecond epoch at which the transaction committed upstream, as reported
  // by the upstream database (e.g. the `commitTime` of the Postgres logical
  // replication Commit message).
  //
  // This is the origin timestamp of the end-to-end serving lag measurement:
  // it is carried through the replication stream to the ViewSyncer, which
  // records `now - commitTimeMs` once the change has been poked to clients.
  //
  // Optional because it is absent from changes written to the Change DB by
  // older versions, which are replayed verbatim during catchup, and from
  // ChangeSources that do not report a commit time.
  commitTimeMs: v.number().optional(),
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
// handled. The is an opaque object that clients must track (and update) based
// on `create-table`, `add-column`, and `table-update-metadata` messages, and
// pass in BackfillRequests when there are columns to be backfilled.
//
// Note that the backfill-related change-source implementation does, however,
// rely on the rowKey (columns) being specified in the message.
export const tableMetadataSchema = v
  .object({rowKey: v.record(jsonValueSchema)})
  .rest(jsonValueSchema);

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
export const backfillIDSchema = jsonObjectSchema;

export type BackfillID = v.Infer<typeof backfillIDSchema>;

export const backfillProgressMarkSchema = v
  .object({
    // A lexicographically sortable string delineating the progress of a
    // backfill. This is used to resume a backfill that did not complete.
    progressMark: v.string(),

    // Optionally, a timeline with which the progressMark is associated.
    // A timeline is an opaque string that determines if backfill streams
    // are "compatible" with each other, i.e. can be merged in a way that
    // avoids gaps. progressMarks from different timelines are not
    // compatible. If two timelines differ, a new backfill is started
    // from scratch in order to get all subscribers on the same timeline.
    //
    // Timeline changes should be rare events (e.g. Postgres VACUUM FULL).
    timeline: v.string().optional(),
  })
  // All other fields should be preserved and returned in BackfillRequests.
  .rest(v.union(v.number(), v.string()));

export type BackfillProgressMark = v.Infer<typeof backfillProgressMarkSchema>;

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

export const downloadStatusSchema = v.object({
  rows: v.number(),
  totalRows: v.number(),
  totalBytes: v.number().optional(),
});

export type DownloadStatus = v.Infer<typeof downloadStatusSchema>;

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

  // Progress marks indicating the position of this backfill message
  // within the stream, for resumption / continuity enforcement.
  //
  // This should be considered non-optional as of protocol v8. It is only
  // absent in messages from pre-v8 replication-managers (including messages
  // persisted in a change-log by one), in which case subscribers accept the
  // message unconditionally (i.e. the legacy behavior).
  progressMarks: v
    .object({
      // The progress mark of the preceding backfill message, from which
      // the current backfill message is a continuation. (This may be a
      // "logical" continuation at which a new backfill stream is started when
      // resuming from a previous stream.)
      //
      // For the first backfill message of a stream starting from scratch,
      // this field is unset. When the field is unset, subscribers should
      // accept the backfill message unconditionally (i.e. starting a
      // backfill from scratch is always safe).
      //
      // If the field is set, subscribers should only accept the message if
      // it is of the same timeline as that of its own tracked progressMark,
      // and is contiguous with or earlier than its own progressMark.
      //
      // In other words, it should _ignore_ backfill messages from other
      // timelines, or messages that create a gap; this can happen when
      // receiving messages from before this subscriber's backfill requests
      // are taken into account.
      //
      // Implementation note: The change-streamer applies this same policy
      // (see `acceptBackfill()` in `protocol/backfill-progress.ts`) to track
      // the backfill state of each subscriber, which it uses to determine the
      // BackfillRequests with which to (re)start change streams.
      previous: backfillProgressMarkSchema.optional(),

      // The progressMark delineating the position of the last row in this
      // message. In the steady state, this will be the `previous`
      // progressMark of the next backfill message, unless a rewind is
      // requested, or new timeline encountered.
      current: backfillProgressMarkSchema,
    })
    .optional(),

  // Optionally includes the progress of the backfill operation,
  // for display purposes.
  status: downloadStatusSchema.optional(),
});

// Indicates that the backfill for the specified columns have
// been successfully backfilled and can be published to clients.
export const backfillCompletedSchema = v.object({
  tag: v.literal('backfill-completed'),

  relation: newRelationSchema,

  // The columns to be backfilled. `rowKey` columns are automatically excluded,
  // which means that this field may be empty.
  columns: v.array(v.string()),

  // The watermark at which the backfill data was queried. Note that this
  // generally will be different from the commit watermarks of the main change
  // stream, and in particular, the commit watermark of the backfill change's
  // enclosing transaction.
  watermark: v.string(),

  // The progress mark of the last backfill message of the run, i.e. the
  // `current` progressMark of the last `backfill` message, or the progressMark
  // from which the run was resumed if it produced no `backfill` messages.
  // Subscribers apply the same continuity policy as for `backfill` messages,
  // and only consider the columns backfilled if the run is contiguous with
  // their own progress.
  //
  // `previous` is only unset for a run that started from scratch and
  // produced no rows (e.g. an empty table), which is always safe to accept.
  //
  // This should be considered non-optional as of protocol v8. It is only
  // absent in messages from pre-v8 replication-managers, in which case
  // subscribers accept the message unconditionally (i.e. legacy behavior).
  progressMarks: v
    .object({previous: backfillProgressMarkSchema.optional()})
    .optional(),

  // Optionally includes the final status of the backfill operation,
  // for display purposes.
  status: downloadStatusSchema.optional(),
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

export const schemaChangeSchema = v.union(...schemaChanges);

const schemaChangeTagsSchema = v.literalUnion(...schemaChangeTags);

export type SchemaChange = Satisfies<
  JSONObject,
  v.Infer<typeof schemaChangeSchema>
>;

export type SchemaChangeTag = v.Infer<typeof schemaChangeTagsSchema>;

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
