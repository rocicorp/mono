import * as v from '@badrap/valita';
import {jsonSchema} from 'reflect-protocol';
import {versionSchema} from 'reflect-protocol';
import type {Storage} from '../storage/storage.js';

export const userValueSchema = v.object({
  version: versionSchema,
  deleted: v.boolean(),
  value: jsonSchema,
});

export type UserValue = v.Infer<typeof userValueSchema>;

export const userValuePrefix = 'user/';

export function userValueKey(key: string): string {
  return `${userValuePrefix}${key}`;
}

export function getUserValue(
  key: string,
  storage: Storage,
): Promise<UserValue | undefined> {
  return storage.get(userValueKey(key), userValueSchema);
}

export function putUserValue(
  key: string,
  value: UserValue,
  storage: Storage,
): Promise<void> {
  return storage.put(userValueKey(key), value);
}
