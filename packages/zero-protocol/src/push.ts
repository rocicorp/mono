import {entityIDSchema} from './entity.js';
import {jsonSchema} from 'shared/src/json-schema.js';
import * as v from 'shared/src/valita.js';

const createOpSchema = v.object({
  op: v.literal('create'),
  entityType: v.string(),
  id: entityIDSchema,
  value: jsonSchema,
});

const setOpSchema = v.object({
  op: v.literal('set'),
  entityType: v.string(),
  id: entityIDSchema,
  value: jsonSchema,
});

const updateOpSchema = v.object({
  op: v.literal('update'),
  entityType: v.string(),
  id: entityIDSchema,
  partialValue: jsonSchema,
});

const deleteOpSchema = v.object({
  op: v.literal('delete'),
  id: entityIDSchema,
});

const crudOpSchema = v.union(
  createOpSchema,
  setOpSchema,
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

export type CreateOp = v.Infer<typeof createOpSchema>;
export type SetOp = v.Infer<typeof setOpSchema>;
export type UpdateOp = v.Infer<typeof updateOpSchema>;
export type DeleteOp = v.Infer<typeof deleteOpSchema>;
export type CRUDOp = v.Infer<typeof crudOpSchema>;
export type CRUDMutationArgs = v.Infer<typeof crudArgsSchema>;
export type CRUDMutation = v.Infer<typeof crudMutationSchema>;
export type CustomMutation = v.Infer<typeof customMutationSchema>;
export type Mutation = v.Infer<typeof mutationSchema>;
export type PushBody = v.Infer<typeof pushBodySchema>;
export type PushMessage = v.Infer<typeof pushMessageSchema>;
