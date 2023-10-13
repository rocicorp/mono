import {jsonSchema} from 'shared/src/json-schema.js';
import * as v from 'shared/src/valita.js';

export const mutationSchema = v.object({
  id: v.number(),
  clientID: v.string(),
  name: v.string(),
  args: jsonSchema,
  timestamp: v.number(),
});

export const pushBodySchema = v.object({
  clientGroupID: v.string(),
  mutations: v.array(mutationSchema),
  pushVersion: v.number(),
  schemaVersion: v.string(),
  timestamp: v.number(),
  requestID: v.string(),
});

export const pushMessageSchema = v.tuple([v.literal('push'), pushBodySchema]);

export type Mutation = v.Infer<typeof mutationSchema>;
export type PushBody = v.Infer<typeof pushBodySchema>;
export type PushMessage = v.Infer<typeof pushMessageSchema>;
