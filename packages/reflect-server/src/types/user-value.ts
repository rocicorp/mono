import * as s from 'superstruct';
import {jsonSchema} from '../protocol/json.js';
import {versionSchema} from './version.js';
import type {Storage} from '../storage/storage.js';

export const userValueSchema = s.type({
  version: versionSchema,
  deleted: s.boolean(),
  value: jsonSchema,
});

export type UserValue = s.Infer<typeof userValueSchema>;

export const userValuePrefix = 'user/';

export function userValueKey(key: string): string {
  return `${userValuePrefix}${key}`;
}

export async function getUserValue(
  key: string,
  storage: Storage,
): Promise<UserValue | undefined> {
  return await storage.get(userValueKey(key), userValueSchema);
}

export async function putUserValue(
  key: string,
  value: UserValue,
  storage: Storage,
): Promise<void> {
  return await storage.put(userValueKey(key), value);
}
