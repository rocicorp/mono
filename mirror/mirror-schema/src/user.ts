import * as v from 'shared/valita.js';
import * as path from './path.js';
import {shortRoleSchema} from './membership.js';

export const userSchema = v.object({
  name: v.string(),

  roles: v.record(shortRoleSchema),
  invites: v.record(shortRoleSchema),
});

export type User = v.Infer<typeof userSchema>;

export const USER_COLLECTION = 'users';

export function userPath(userID: string): string {
  return path.join(USER_COLLECTION, userID);
}
