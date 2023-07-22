import {jsonSchema, versionSchema, Version} from 'reflect-protocol';
import * as v from 'shared/src/valita.js';
import type {Storage} from '../storage/storage.js';
import {versionToLexi, versionFromLexi} from './lexi-version.js';
import {assert} from 'shared/src/asserts.js';

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

export const userValueVersionInfoSchema = v.object({
  deleted: v.boolean().optional(),
});

export type UserValueVersionInfo = v.Infer<typeof userValueVersionInfoSchema>;

export const userValueVersionIndexPrefix = 'v/';

export function userValueVersionKey(userKey: string, version: Version): string {
  const lexiVersion = versionToLexi(version);
  return `${userValueVersionIndexPrefix}${lexiVersion}/${userKey}`;
}

export function userValueVersionEntry(
  userKey: string,
  userValue: UserValue,
): {key: string; value: UserValueVersionInfo} {
  const key = userValueVersionKey(userKey, userValue.version);
  const value = userValue.deleted ? {deleted: true} : {};
  return {key, value};
}

const VALUE_VERSION_PREFIX_LENGTH = userValueVersionIndexPrefix.length;

export function decodeUserValueVersionKey(indexKey: string): {
  userKey: string;
  version: Version;
} {
  assert(
    indexKey.startsWith(userValueVersionIndexPrefix),
    `Invalid version index key: ${indexKey}`,
  );
  const suffix = indexKey.substring(VALUE_VERSION_PREFIX_LENGTH);
  const firstSlash = suffix.indexOf('/');
  assert(firstSlash > 0, `Invalid version index key: ${indexKey}`);

  return {
    userKey: suffix.substring(firstSlash + 1),
    version: versionFromLexi(suffix.substring(0, firstSlash)),
  };
}
