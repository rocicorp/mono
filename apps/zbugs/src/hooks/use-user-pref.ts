import {useQuery} from '@rocicorp/zero/react';
import type {ZeroBugs} from '../../shared/zero-type.ts';
import {useZero} from './use-zero.ts';

export function useUserPref(key: string): string | undefined {
  const z = useZero();
  const [pref] = useQuery(z.query.userPref(key));
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
