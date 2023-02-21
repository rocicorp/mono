import * as s from 'superstruct';
import {nullableVersionSchema, versionSchema} from '../types/version.js';
import {patchSchema} from './patch.js';

export const pullRequestSchema = s.object({
  roomID: s.string(),
  profileID: s.string(),
  clientGroupID: s.string(),
  cookie: nullableVersionSchema,
  pullVersion: s.number(),
  schemaVersion: s.string(),
});

export const pullResponseSchema = s.object({
  cookie: versionSchema,
  lastMutationIDChanges: s.record(s.string(), s.number()),
  // Pull is only used for mutation recovery which does not use
  // the patch so we save work by not computing the patch.
  patch: s.size(patchSchema, 0, 0),
});

export type PullRequest = s.Infer<typeof pullRequestSchema>;
export type PullResponse = s.Infer<typeof pullResponseSchema>;
