import type {Zero} from '@rocicorp/zero';
import {useQuery} from '@rocicorp/zero/react';
import {mutators} from '../../shared/mutators.ts';
import {queries} from '../../shared/queries.ts';

export function useUserPref(key: string): string | undefined {
  const [pref] = useQuery(queries.userPref(key));
  return pref?.value;
}

export async function setUserPref(
  z: Zero,
  key: string,
  value: string,
): Promise<void> {
  await z.mutate(mutators.userPref.set({key, value})).client;
}

export function useNumericPref(key: string, defaultValue: number): number {
  const value = useUserPref(key);
  return value !== undefined ? parseInt(value, 10) : defaultValue;
}

export function setNumericPref(
  z: Zero,
  key: string,
  value: number,
): Promise<void> {
  return setUserPref(z, key, value + '');
}
