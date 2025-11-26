import {useQuery} from '@rocicorp/zero/react';
import {queries} from '../../shared/queries.ts';
import type {ZeroBugs} from '../../shared/zero-type.ts';

export function useUserPref(key: string): string | undefined {
  const [pref] = useQuery(queries.userPref(key));
  return pref?.value;
}

export async function setUserPref(
  z: ZeroBugs,
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
  z: ZeroBugs,
  key: string,
  value: number,
): Promise<void> {
  return setUserPref(z, key, value + '');
}
