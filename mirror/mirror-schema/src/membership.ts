import * as v from 'shared/valita.js';
import * as path from './path.js';
import {teamPath} from './team.js';

export const roleSchema = v.union(v.literal('member'), v.literal('admin'));
export type Role = v.Infer<typeof roleSchema>;

export const shortRoleSchema = v.union(v.literal('m'), v.literal('a'));
export type ShortRole = v.Infer<typeof shortRoleSchema>;

export const membershipSchema = v.object({
  role: roleSchema,
  email: v.string(),
});
export type Membership = v.Infer<typeof membershipSchema>;

export const TEAM_MEMBERSHIPS_COLLECTION_ID = 'memberships';
export const TEAM_INVITES_COLLECTION_ID = 'invites';

export function teamMembershipsCollection(teamID: string): string {
  return path.append(teamPath(teamID), TEAM_MEMBERSHIPS_COLLECTION_ID);
}

export function teamMembershipPath(teamID: string, userID: string): string {
  return path.append(teamMembershipsCollection(teamID), userID);
}

export function teamInvitesCollection(teamID: string): string {
  return path.append(teamPath(teamID), TEAM_INVITES_COLLECTION_ID);
}

export function teamInvitePath(teamID: string, userID: string): string {
  return path.append(teamInvitesCollection(teamID), userID);
}
