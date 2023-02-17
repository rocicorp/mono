import {nullableVersionSchema, versionSchema} from '../types/version.js';
import * as s from 'superstruct';

export const pullRequestSchema = s.type({
  roomID: s.string(),
  profileID: s.string(),
  clientGroupID: s.string(),
  cookie: nullableVersionSchema,
  pullVersion: s.number(),
  schemaVersion: s.string(),
});

export const pullResponseSchema = s.type({
  cookie: versionSchema,
  lastMutationIDChanges: s.record(s.string(), s.number()),
  // Pull is only used for mutation recovery which does not use
  // the patch so we save work by not computing the patch.
  patch: s.empty(s.array()),
});

export type PullRequest = s.Infer<typeof pullRequestSchema>;
export type PullResponse = s.Infer<typeof pullResponseSchema>;
