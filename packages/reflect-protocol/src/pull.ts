import * as s from 'superstruct';
import {nullableVersionSchema, versionSchema} from './version.js';

export const pullRequestBodySchema = s.object({
  clientGroupID: s.string(),
  cookie: nullableVersionSchema,
  requestID: s.string(),
});

export const pullResponseBodySchema = s.object({
  cookie: versionSchema,
  // Matches pullRequestBodySchema requestID that initiated this response
  requestID: s.string(),
  lastMutationIDChanges: s.record(s.string(), s.number()),
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
