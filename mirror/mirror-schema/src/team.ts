import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import * as path from './path.js';

export const teamSchema = v.object({
  name: v.string(),
  defaultCfID: v.string(),

  // Number of memberships of role 'admin'.
  // A team must have at least one admin.
  numAdmins: v.number(),
  // Number of memberships of role 'member'.
  numMembers: v.number(),
  numInvites: v.number(),

  numApps: v.number(),
  // null means default max
  maxApps: v.union(v.number(), v.null()),
});

export type Team = v.Infer<typeof teamSchema>;

export const teamDataConverter = firestoreDataConverter(teamSchema);

export const TEAM_COLLECTION = 'teams';

export function teamPath(teamID: string): string {
  return path.join(TEAM_COLLECTION, teamID);
}
