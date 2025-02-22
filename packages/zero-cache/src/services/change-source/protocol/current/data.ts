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
} from '../../../../types/bigint-json.ts';
import type {Satisfies} from '../../../../types/satisfies.ts';

export const beginSchema = v.object({
  tag: v.literal('begin'),
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

  // Deprecated tags will be removed in a later release.
  tag: v.literal('relation').optional(),
  replicaIdentity: v
    .union(
      v.literal('default'),
      v.literal('nothing'),
      v.literal('full'),
      v.literal('index'),
    )
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
  key: rowSchema.nullable(),
  old: rowSchema.nullable(),
  new: rowSchema,
});

export const deleteSchema = v.object({
  tag: v.literal('delete'),
  relation: relationSchema,
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
