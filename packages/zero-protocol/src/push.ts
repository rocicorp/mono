import {jsonSchema} from 'shared/src/json-schema.js';
import * as v from 'shared/src/valita.js';

const insertOpSchema = v.object({
  op: v.literal('insert'),
  entityType: v.string(),
  id: v.record(v.string()),
  value: jsonSchema,
});

const upsertOpSchema = v.object({
  op: v.literal('upsert'),
  entityType: v.string(),
  id: v.record(v.string()),
  value: jsonSchema,
});

const updateOpSchema = v.object({
  op: v.literal('update'),
  entityType: v.string(),
  id: v.record(v.string()),
  partialValue: jsonSchema,
});

const deleteOpSchema = v.object({
  op: v.literal('delete'),
  /* attributeName => value */
  id: v.record(v.string()),
});

const crudOpSchema = v.union(
  insertOpSchema,
  upsertOpSchema,
  updateOpSchema,
  deleteOpSchema,
);

const crudArgsSchema = v.array(crudOpSchema);

export const crudMutationSchema = v.object({
  id: v.number(),
  clientID: v.string(),
  name: v.literal('_zero_crud'),
  args: crudArgsSchema,
  timestamp: v.number(),
});

export const customMutationSchema = v.object({
  id: v.number(),
  clientID: v.string(),
  name: v.string(),
  args: v.array(jsonSchema),
  timestamp: v.number(),
});

export const mutationSchema = v.union(crudMutationSchema, customMutationSchema);

export const pushBodySchema = v.object({
  clientGroupID: v.string(),
  mutations: v.array(mutationSchema),
  pushVersion: v.number(),
  schemaVersion: v.string(),
  timestamp: v.number(),
  requestID: v.string(),
});

export const pushMessageSchema = v.tuple([v.literal('push'), pushBodySchema]);

export type InsertOp = v.Infer<typeof insertOpSchema>;
export type UpsertOp = v.Infer<typeof upsertOpSchema>;
export type UpdateOp = v.Infer<typeof updateOpSchema>;
export type DeleteOp = v.Infer<typeof deleteOpSchema>;
export type CRUDOp = v.Infer<typeof crudOpSchema>;
export type CRUDMutationArgs = v.Infer<typeof crudArgsSchema>;
export type CRUDMutation = v.Infer<typeof crudMutationSchema>;
export type CustomMutation = v.Infer<typeof customMutationSchema>;
export type Mutation = v.Infer<typeof mutationSchema>;
export type PushBody = v.Infer<typeof pushBodySchema>;
export type PushMessage = v.Infer<typeof pushMessageSchema>;
