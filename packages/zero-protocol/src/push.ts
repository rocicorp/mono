import {jsonObjectSchema, jsonSchema} from 'shared/src/json-schema.js';
import * as v from 'shared/src/valita.js';
import {entityIDSchema} from './entity.js';

/**
 * Inserts if entity with id does not already exist.
 */
const createOpSchema = v.object({
  op: v.literal('create'),
  entityType: v.string(),
  id: entityIDSchema,
  value: jsonObjectSchema,
});

/**
 * Upsert semantics. Inserts if entity with id does not already exist,
 * otherwise updates existing entity with id.
 */
const setOpSchema = v.object({
  op: v.literal('set'),
  entityType: v.string(),
  id: entityIDSchema,
  value: jsonObjectSchema,
});

/**
 * Updates if entity with id exists, otherwise does nothing.
 */
const updateOpSchema = v.object({
  op: v.literal('update'),
  entityType: v.string(),
  id: entityIDSchema,
  partialValue: jsonObjectSchema,
});

/**
 * Deletes entity with id if it exists, otherwise does nothing.
 */
const deleteOpSchema = v.object({
  op: v.literal('delete'),
  entityType: v.string(),
  id: entityIDSchema,
});

const crudOpSchema = v.union(
  createOpSchema,
  setOpSchema,
  updateOpSchema,
  deleteOpSchema,
);

const crudArgSchema = v.object({
  ops: v.array(crudOpSchema),
});

const crudArgsSchema = v.tuple([crudArgSchema]);

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
export type CRUDOpKind = CRUDOp['op'];
export type CRUDMutationArg = v.Infer<typeof crudArgSchema>;
export type CRUDMutation = v.Infer<typeof crudMutationSchema>;
export type CustomMutation = v.Infer<typeof customMutationSchema>;
export type Mutation = v.Infer<typeof mutationSchema>;
export type PushBody = v.Infer<typeof pushBodySchema>;
export type PushMessage = v.Infer<typeof pushMessageSchema>;
