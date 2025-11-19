import type {Zero} from '@rocicorp/zero';
import {useQuery} from '@rocicorp/zero/react';
import type {AuthData} from '../../shared/auth.ts';
import type {Mutators} from '../../shared/mutators.ts';
import {queries} from '../../shared/queries.ts';
import {type Schema} from '../../shared/schema.ts';

export function useUserPref(key: string): string | undefined {
  const [pref] = useQuery(queries.userPref(key));
  return pref?.value;
}

export async function setUserPref(
  z: Zero<Schema, Mutators, AuthData | undefined>,
  key: string,
  value: string,
  mutate = z.mutate,
): Promise<void> {
  await mutate.userPref.set({key, value}).client;
}

export function useNumericPref(key: string, defaultValue: number): number {
  const value = useUserPref(key);
  return value !== undefined ? parseInt(value, 10) : defaultValue;
}

export function setNumericPref(
  z: Zero<Schema, Mutators, AuthData | undefined>,
  key: string,
  value: number,
): Promise<void> {
  return setUserPref(z, key, value + '');
}
