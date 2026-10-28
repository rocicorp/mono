import * as v from '../../shared/src/valita.ts';
import {nullableVersionSchema, versionSchema} from './version.ts';

const pullRequestBodySchema = v.object({
  clientGroupID: v.string(),
  cookie: nullableVersionSchema,
  requestID: v.string(),
});

const pullResponseBodySchema = v.object({
  cookie: versionSchema,
  // Matches pullRequestBodySchema requestID that initiated this response
  requestID: v.string(),
  lastMutationIDChanges: v.record(v.number()),
  // Pull is currently only used for mutation recovery which does not use
  // the patch so we save work by not computing the patch.
});

export const pullRequestMessageSchema = v.tuple([
  v.literal('pull'),
  pullRequestBodySchema,
]);

export const pullResponseMessageSchema = v.tuple([
  v.literal('pull'),
  pullResponseBodySchema,
]);

export type PullResponseBody = v.Infer<typeof pullResponseBodySchema>;

export type PullRequestMessage = v.Infer<typeof pullRequestMessageSchema>;
export type PullResponseMessage = v.Infer<typeof pullResponseMessageSchema>;
