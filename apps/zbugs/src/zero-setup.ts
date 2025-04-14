import {Zero} from '@rocicorp/zero';
import {type Schema, schema} from '../shared/schema.ts';
import {createMutators, type Mutators} from '../shared/mutators.ts';
import {Atom} from './atom.ts';
import {clearJwt, getJwt, getRawJwt} from './jwt.ts';
import {mark} from './perf-log.ts';
import {CACHE_FOREVER} from './query-cache-policy.ts';
import type {AuthData} from '../shared/auth.ts';

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
    push: {
      url: 'http://localhost:5173/api/push?foo=bar',
    },
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

  // Initially preload just open issues, the minimum we need to show the
  // homepage.
  //
  // TODO: we can actually do better, we should really preload only first few
  // pages and use infinite scroll. In practice for zbugs there are few enough
  // open issues that loading all is fine. It would still be better to do
  // infinite scroll for demo/educational purposes.
  const baseIssueQuery = z.query.issue
    .where('open', true)
    .related('labels')
    .related('viewState', q => q.where('userID', z.userID));

  // One the bare issues are loaded, also preload the details needed for the
  // issue page, so that navigating into details is also fast.
  const {complete, cleanup} = baseIssueQuery.preload(CACHE_FOREVER);
  complete.then(() => {
    cleanup();
    mark('preload complete');
    baseIssueQuery
      .related('creator')
      .related('assignee')
      .related('emoji', emoji => emoji.related('creator'))
      .related('comments', comments =>
        comments
          .related('creator')
          .related('emoji', emoji => emoji.related('creator'))
          .limit(10)
          .orderBy('created', 'desc'),
      )
      .preload(CACHE_FOREVER);
  });

  z.query.user.preload(CACHE_FOREVER);
  z.query.label.preload(CACHE_FOREVER);
}

// To enable accessing zero in the devtools easily.
function exposeDevHooks(z: Zero<Schema, Mutators>) {
  const casted = window as unknown as {
    z?: Zero<Schema, Mutators>;
  };
  casted.z = z;
}

export {authAtom as authRef, zeroAtom as zeroRef};
