import * as v from '../../../../../shared/src/valita.js';
import {jsonValueSchema, type JSONObject} from '../../../types/bigint-json.js';
import type {Satisfies} from '../../../types/satisfies.js';

export const beginSchema = v.object({
  tag: v.literal('begin'),
});

export const commitSchema = v.object({
  tag: v.literal('commit'),
});

export const relationSchema = v.object({
  tag: v.literal('relation'),
  schema: v.string(),
  name: v.string(),
  replicaIdentity: v.union(
    v.literal('default'),
    v.literal('nothing'),
    v.literal('full'),
    v.literal('index'),
  ),
  keyColumns: v.array(v.string()),
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

export type MessageBegin = v.Infer<typeof beginSchema>;

export type MessageCommit = v.Infer<typeof commitSchema>;

export type MessageRelation = v.Infer<typeof relationSchema>;

export type MessageInsert = v.Infer<typeof insertSchema>;

export type MessageUpdate = v.Infer<typeof updateSchema>;

export type MessageDelete = v.Infer<typeof deleteSchema>;

export type MessageTruncate = v.Infer<typeof truncateSchema>;

export const dataChangeSchema = v.union(
  insertSchema,
  updateSchema,
  deleteSchema,
  truncateSchema,
);

export type DataChange = Satisfies<
  JSONObject, // guarantees serialization over IPC or network
  v.Infer<typeof dataChangeSchema>
>;

export type Change = MessageBegin | DataChange | MessageCommit;

export type ChangeTag = Change['tag'];
