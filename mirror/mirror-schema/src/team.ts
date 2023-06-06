import * as v from 'shared/valita.js';
import * as path from './path.js';

export const teamSchema = v.object({
  name: v.string(),
  defaultCfID: v.string(),

  admins: v.array(v.string()),
  members: v.array(v.string()),
  invites: v.array(v.string()).optional(),

  numApps: v.number(),
  // null means default max
  maxApps: v.union(v.number(), v.null()),
});

export type Team = v.Infer<typeof teamSchema>;

export const TEAM_COLLECTION = 'teams';

export function teamPath(teamID: string): string {
  return path.join(TEAM_COLLECTION, teamID);
}
