import {nullableVersionSchema, versionSchema} from '../types/version.js';
import * as s from 'superstruct';

export const pullRequestBodySchema = s.object({
  clientGroupID: s.string(),
  cookie: nullableVersionSchema,
  requestID: s.string(),
});

export const pullResponseBodySchema = s.object({
  cookie: versionSchema,
  lastMutationIDChanges: s.record(s.string(), s.number()),
  requestID: s.string(),
  // Pull is currently only used for mutation recovery which does not use
  // the patch so we save work by not computing the patch.
});

export const pullRequestMessageSchema = s.tuple([
  s.literal('pull'),
  pullRequestBodySchema,
]);

export const pullResponseMessageSchema = s.tuple([
  s.literal('pull'),
  pullResponseBodySchema,
]);

export type PullRequestBody = s.Infer<typeof pullRequestBodySchema>;
export type PullResponseBody = s.Infer<typeof pullResponseBodySchema>;

export type PullRequestMessage = s.Infer<typeof pullRequestMessageSchema>;
export type PullResponseMessage = s.Infer<typeof pullResponseMessageSchema>;
