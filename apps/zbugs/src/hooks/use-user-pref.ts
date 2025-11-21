import type {Zero} from '@rocicorp/zero';
import {useQuery, useZero} from '@rocicorp/zero/react';

export function useUserPref(key: string): string | undefined {
  const z = useZero();
  const [pref] = useQuery(z.query.userPref(key));
  return pref?.value;
}

export async function setUserPref(
  z: Zero,
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
  z: Zero,
  key: string,
  value: number,
): Promise<void> {
  return setUserPref(z, key, value + '');
}
