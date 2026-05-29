import * as v from 'shared/src/valita.ts';
import {pushFailedBodySchema} from './error.ts';
import {mutationIDSchema} from './mutation-id.ts';
import {mutationResponseSchema, mutationSchema} from './mutation.ts';

export const pushBodySchema = v.object({
  clientGroupID: v.string(),
  mutations: v.array(mutationSchema),
  pushVersion: v.number(),
  // For legacy (CRUD) mutations, the schema is tied to the client group /
  // sync connection. For custom mutations, schema versioning is delegated
  // to the custom protocol / api-server.
  schemaVersion: v.number().optional(),
  timestamp: v.number(),
  requestID: v.string(),
  /**
   * @deprecated auth is managed at client-group scope via connect/updateAuth
   * and should not be included in push messages.
   */
  auth: v.string().optional(),
  /** W3C traceparent header for distributed tracing. */
  traceparent: v.string().optional(),
});

export const pushMessageSchema = v.tuple([v.literal('push'), pushBodySchema]);

const pushOkSchema = v.object({
  mutations: v.array(mutationResponseSchema),
});

/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
const unsupportedPushVersionSchema = v.object({
  /** @deprecated */
  error: v.literal('unsupportedPushVersion'),
  /** @deprecated */
  mutationIDs: v.array(mutationIDSchema).optional(),
});
/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
const unsupportedSchemaVersionSchema = v.object({
  /** @deprecated */
  error: v.literal('unsupportedSchemaVersion'),
  /** @deprecated */
  mutationIDs: v.array(mutationIDSchema).optional(),
});
/**
 * @deprecated push http errors are now represented as ['error', { ... }] messages
 */
const httpErrorSchema = v.object({
  /** @deprecated */
  error: v.literal('http'),
  /** @deprecated */
  status: v.number(),
  /** @deprecated */
  details: v.string(),
  /** @deprecated */
  mutationIDs: v.array(mutationIDSchema).optional(),
});
/**
 * @deprecated push zero errors are now represented as ['error', { ... }] messages
 */
const zeroPusherErrorSchema = v.object({
  /** @deprecated */
  error: v.literal('zeroPusher'),
  /** @deprecated */
  details: v.string(),
  /** @deprecated */
  mutationIDs: v.array(mutationIDSchema).optional(),
});
/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
export const pushErrorSchema = v.union(
  unsupportedPushVersionSchema,
  unsupportedSchemaVersionSchema,
  httpErrorSchema,
  zeroPusherErrorSchema,
);

export const pushResponseBodySchema = v.union(pushOkSchema, pushErrorSchema);

export const pushResponseSchema = v.union(
  pushResponseBodySchema,
  pushFailedBodySchema,
);
export const pushResponseMessageSchema = v.tuple([
  v.literal('pushResponse'),
  pushResponseBodySchema,
]);

export const ackMutationResponsesMessageSchema = v.tuple([
  v.literal('ackMutationResponses'),
  mutationIDSchema,
]);

export type PushBody = v.Infer<typeof pushBodySchema>;
export type PushMessage = v.Infer<typeof pushMessageSchema>;
export type PushResponseBody = v.Infer<typeof pushResponseBodySchema>;
export type PushResponse = v.Infer<typeof pushResponseSchema>;
export type PushResponseMessage = v.Infer<typeof pushResponseMessageSchema>;
export type MutationResponse = v.Infer<typeof mutationResponseSchema>;
/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
export type PushError = v.Infer<typeof pushErrorSchema>;
export type PushOk = v.Infer<typeof pushOkSchema>;
export type AckMutationMessage = v.Infer<
  typeof ackMutationResponsesMessageSchema
>;
