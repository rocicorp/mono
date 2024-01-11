import * as v from 'shared/src/valita.js';
import {createCall} from '../functions/internal/call.js';

export const updateKeyRequestSchema = v.object({
  appID: v.string(),
  keyName: v.string(),
  lastUsed: v.number(),
});

export const updateBatchSchema = v.object({
  updates: v.record(v.number()),
  coalesced: v.number(),
});

export const updateKeyResponseSchema = v.object({
  flushed: updateBatchSchema.optional(),
});

export type UpdateBatch = v.Infer<typeof updateBatchSchema>;
export type UpdateKeyRequest = v.Infer<typeof updateKeyRequestSchema>;
export type UpdateKeyResponse = v.Infer<typeof updateKeyResponseSchema>;

export const updateKey = createCall(
  'appKeys-update',
  updateKeyRequestSchema,
  updateKeyResponseSchema,
);
