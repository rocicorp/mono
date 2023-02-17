import {z} from 'zod';
import {nullableVersionSchema, versionSchema} from '../types/version.js';
import {patchSchema} from './patch.js';

export const pullRequestSchema = z.object({
  roomID: z.string(),
  profileID: z.string(),
  clientGroupID: z.string(),
  cookie: nullableVersionSchema,
  pullVersion: z.number(),
  schemaVersion: z.string(),
});

export const pullResponseSchema = z.object({
  cookie: versionSchema,
  lastMutationIDChanges: z.record(z.string(), z.number()),
  // Pull is only used for mutation recovery which does not use
  // the patch so we save work by not computing the patch.
  patch: patchSchema.length(0),
});

export type PullRequest = z.infer<typeof pullRequestSchema>;
export type PullResponse = z.infer<typeof pullResponseSchema>;
