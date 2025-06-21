/**
 * Data plane messages encapsulate changes that are sent by ChangeSources,
 * forwarded / fanned out to subscribers by the ChangeStreamerService, and
 * stored in the Change DB for catchup of old subscribers.
 */

import * as v from '../../../../../../shared/src/valita.ts';
import {columnSpec, indexSpec, tableSpec} from '../../../../db/specs.ts';
import {
  jsonValueSchema,
  type JSONObject,
} from '../../../../../../shared/src/bigint-json.ts';
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

export const relationSchema = v.object({
  schema: v.string(),
  name: v.string(),
  keyColumns: v.array(v.string()),

  // PG-specific. When replicaIdentity is 'full':
  // * `keyColumns` contain all of the columns in the table.
  // * the `key` of the Delete and Update messages represent the full row.
  //
  // The replicator handles these tables by extracting a row key from
  // the full row based on the table's PRIMARY KEY or UNIQUE INDEX.
  replicaIdentity: v
    .literalUnion('default', 'nothing', 'full', 'index')
    .optional(),
});

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

const identifierSchema = v.object({
  schema: v.string(),
  name: v.string(),
});

export type Identifier = v.Infer<typeof identifierSchema>;

export const createTableSchema = v.object({
  tag: v.literal('create-table'),
  spec: tableSpec,
});

export const renameTableSchema = v.object({
  tag: v.literal('rename-table'),
  old: identifierSchema,
  new: identifierSchema,
});

const columnSchema = v.object({
  name: v.string(),
  spec: columnSpec,
});

export const addColumnSchema = v.object({
  tag: v.literal('add-column'),
  table: identifierSchema,
  column: columnSchema,
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

export type MessageBegin = v.Infer<typeof beginSchema>;
export type MessageCommit = v.Infer<typeof commitSchema>;
export type MessageRollback = v.Infer<typeof rollbackSchema>;

export type MessageRelation = v.Infer<typeof relationSchema>;
export type MessageInsert = v.Infer<typeof insertSchema>;
export type MessageUpdate = v.Infer<typeof updateSchema>;
export type MessageDelete = v.Infer<typeof deleteSchema>;
export type MessageTruncate = v.Infer<typeof truncateSchema>;

export type TableCreate = v.Infer<typeof createTableSchema>;
export type TableRename = v.Infer<typeof renameTableSchema>;
export type ColumnAdd = v.Infer<typeof addColumnSchema>;
export type ColumnUpdate = v.Infer<typeof updateColumnSchema>;
export type ColumnDrop = v.Infer<typeof dropColumnSchema>;
export type TableDrop = v.Infer<typeof dropTableSchema>;
export type IndexCreate = v.Infer<typeof createIndexSchema>;
export type IndexDrop = v.Infer<typeof dropIndexSchema>;

export const dataChangeSchema = v.union(
  insertSchema,
  updateSchema,
  deleteSchema,
  truncateSchema,
  createTableSchema,
  renameTableSchema,
  addColumnSchema,
  updateColumnSchema,
  dropColumnSchema,
  dropTableSchema,
  createIndexSchema,
  dropIndexSchema,
);

export type DataChange = Satisfies<
  JSONObject, // guarantees serialization over IPC or network
  v.Infer<typeof dataChangeSchema>
>;

export type Change =
  | MessageBegin
  | DataChange
  | MessageCommit
  | MessageRollback;

export type ChangeTag = Change['tag'];
