import type {Zero} from '@rocicorp/zero';
import {useQuery} from '@rocicorp/zero/react';
import {type Schema} from '../../shared/schema.ts';
import {useZero} from './use-zero.ts';
import {userPref} from '../../shared/queries.ts';
import {setUserPref as setUserPrefMutation} from '../../shared/mutators.ts';
import type {JWTData} from '../../shared/auth.ts';

export function useUserPref(key: string): string | undefined {
  const z = useZero();
  const [pref] = useQuery(userPref(key, z.userID));
  return pref?.value;
}

export async function setUserPref(
  z: Zero<Schema>,
  authData: JWTData | undefined,
  key: string,
  value: string,
): Promise<void> {
  setUserPrefMutation(authData)(z, {key, value});
}

export function useNumericPref(key: string, defaultValue: number): number {
  const value = useUserPref(key);
  return value !== undefined ? parseInt(value, 10) : defaultValue;
}

export function setNumericPref(
  z: Zero<Schema>,
  authData: JWTData | undefined,
  key: string,
  value: number,
): Promise<void> {
  return setUserPref(z, authData, key, value + '');
}
