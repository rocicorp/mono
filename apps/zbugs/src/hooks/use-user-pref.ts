import type {Zero} from '@rocicorp/zero';
import {useQuery} from '@rocicorp/zero/react';
import type {Schema} from '../../schema.js';
import {useZero} from './use-zero.js';

export function useUserPref(key: string): string | undefined {
  const z = useZero();
  const q = z.query.userPref.where('key', key).where('userID', z.userID);
  return useQuery(q.one())?.value;
}

export async function setUserPref(
  z: Zero<Schema>,
  key: string,
  value: string,
  mutate = z.mutate,
): Promise<void> {
  await mutate.userPref.upsert({key, value, userID: z.userID});
}

export function useNumericPref(key: string, defaultValue: number): number {
  const value = useUserPref(key);
  return value !== undefined ? parseInt(value, 10) : defaultValue;
}

export function setNumericPref(
  z: Zero<Schema>,
  key: string,
  value: number,
): Promise<void> {
  return setUserPref(z, key, value + '');
}
