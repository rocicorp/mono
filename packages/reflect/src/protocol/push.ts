import {z} from 'zod';
import {jsonSchema} from './json.js';

export const mutationSchema = z.object({
  id: z.number(),
  clientID: z.string(),
  name: z.string(),
  args: jsonSchema,
  timestamp: z.number(),
});

export const pushBodySchema = z.object({
  clientGroupID: z.string(),
  mutations: z.array(mutationSchema),
  pushVersion: z.number(),
  schemaVersion: z.string(),
  timestamp: z.number(),
  requestID: z.string(),
});

export const pushMessageSchema = z.tuple([z.literal('push'), pushBodySchema]);

export type Mutation = z.infer<typeof mutationSchema>;
export type PushBody = z.infer<typeof pushBodySchema>;
export type PushMessage = z.infer<typeof pushMessageSchema>;
