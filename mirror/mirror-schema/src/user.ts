import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import {roleSchema} from './membership.js';
import * as path from './path.js';

export const userSchema = v.object({
  email: v.string(),
  name: v.string().optional(),

  roles: v.record(roleSchema),
  invites: v.record(roleSchema).optional(),
});

export type User = v.Infer<typeof userSchema>;

export const userDataConverter = firestoreDataConverter(userSchema);

export const USER_COLLECTION = 'users';

export function userPath(userID: string): string {
  return path.join(USER_COLLECTION, userID);
}
