import {Zero} from '@rocicorp/zero';
import {type Schema, schema} from '../shared/schema.ts';
import {createMutators, type Mutators} from '../shared/mutators.ts';
import {Atom} from './atom.ts';
import {clearJwt, getJwt, getRawJwt} from './jwt.ts';
import {mark} from './perf-log.ts';
import {CACHE_FOREVER} from './query-cache-policy.ts';
import type {AuthData} from '../shared/auth.ts';
import {allLabels, allUsers, issuePreload} from '../shared/queries.ts';

export type LoginState = {
  encoded: string;
  decoded: AuthData;
};

const zeroAtom = new Atom<Zero<Schema, Mutators>>();
const authAtom = new Atom<LoginState>();
const jwt = getJwt();
const encodedJwt = getRawJwt();

authAtom.value =
  encodedJwt && jwt
    ? {
        encoded: encodedJwt,
        decoded: jwt as LoginState['decoded'],
      }
    : undefined;

authAtom.onChange(auth => {
  zeroAtom.value?.close();
  mark('creating new zero');
  const authData = auth?.decoded;
  const z = new Zero({
    logLevel: 'info',
    server: import.meta.env.VITE_PUBLIC_SERVER,
    userID: authData?.sub ?? 'anon',
    mutators: createMutators(authData),
    auth: (error?: 'invalid-token') => {
      if (error === 'invalid-token') {
        clearJwt();
        authAtom.value = undefined;
        return undefined;
      }
      return auth?.encoded;
    },
    schema,
  });
  zeroAtom.value = z;

  exposeDevHooks(z);
});

let didPreload = false;

export function preload(z: Zero<Schema, Mutators>) {
  if (didPreload) {
    return;
  }

  didPreload = true;

  // Preload all issues and first 10 comments from each.
  z.preload(issuePreload(z.userID), CACHE_FOREVER);

  z.preload(allUsers(), CACHE_FOREVER);
  z.preload(allLabels(), CACHE_FOREVER);
}

// To enable accessing zero in the devtools easily.
function exposeDevHooks(z: Zero<Schema, Mutators>) {
  const casted = window as unknown as {
    z?: Zero<Schema, Mutators>;
  };
  casted.z = z;
}

export {authAtom as authRef, zeroAtom as zeroRef};
