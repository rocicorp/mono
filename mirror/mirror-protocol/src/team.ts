import * as v from 'shared/src/valita.js';
import {baseRequestFields, baseResponseFields} from './base.js';
import {createCaller} from './call.js';

export const ensureTeamRequestSchema = v.object({
  ...baseRequestFields,
  name: v.string(),
});
export type EnsureTeamRequest = v.Infer<typeof ensureTeamRequestSchema>;

export const ensureTeamResponseSchema = v.object({
  ...baseResponseFields,
  teamID: v.string(),
});
export type EnsureTeamResponse = v.Infer<typeof ensureTeamResponseSchema>;

// The team-ensure function ensures that the user is part of exactly one Team,
// initializing one with the given name if necessary. The name should default
// to the user's github username, which is available on the client credentials
// via the github oauth login.
export const ensureTeam = createCaller(
  'team-ensure',
  ensureTeamRequestSchema,
  ensureTeamResponseSchema,
);
