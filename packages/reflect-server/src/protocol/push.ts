import * as s from 'superstruct';
import {jsonSchema} from './json.js';

export const mutationSchema = s.type({
  id: s.number(),
  clientID: s.string(),
  name: s.string(),
  args: jsonSchema,
  timestamp: s.number(),
});

export const pushBodySchema = s.type({
  clientGroupID: s.string(),
  mutations: s.array(mutationSchema),
  pushVersion: s.number(),
  schemaVersion: s.string(),
  timestamp: s.number(),
  requestID: s.string(),
});

export const pushMessageSchema = s.tuple([s.literal('push'), pushBodySchema]);

export type Mutation = s.Infer<typeof mutationSchema>;
export type PushBody = s.Infer<typeof pushBodySchema>;
export type PushMessage = s.Infer<typeof pushMessageSchema>;
