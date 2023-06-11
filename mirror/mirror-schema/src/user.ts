import * as v from 'shared/valita.js';
import * as path from './path.js';
import {shortRoleSchema} from './membership.js';
import {firestoreDataConverter} from './converter.js';

export const userSchema = v.object({
  email: v.string(),
  name: v.string().optional(),

  roles: v.record(shortRoleSchema),
  invites: v.record(shortRoleSchema).optional(),
});

export type User = v.Infer<typeof userSchema>;

export const userDataConverter = firestoreDataConverter(userSchema);

export const USER_COLLECTION = 'users';

export function userPath(userID: string): string {
  return path.join(USER_COLLECTION, userID);
}
