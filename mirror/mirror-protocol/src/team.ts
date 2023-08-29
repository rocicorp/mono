import * as v from 'shared/src/valita.js';
import {baseRequestFields, baseResponseFields} from './base.js';
import {createCall} from './call.js';

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

export const ensureTeam = createCall(
  'team-ensure',
  ensureTeamRequestSchema,
  ensureTeamResponseSchema,
);
