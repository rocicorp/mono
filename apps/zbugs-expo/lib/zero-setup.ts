import { Zero } from '@rocicorp/zero';
import { createReplicacheExpoSQLiteKVStore } from '@rocicorp/zero/expo';
import { type Schema, schema } from './schema';
import { Atom } from '@/lib/atom';
import { clearJwt, getJwt, getRawJwt } from '@/lib/jwt';
import { mark } from '@/lib/perf-log';

export type LoginState = {
  encoded: string;
  decoded: {
    sub: string;
    name: string;
    role: 'crew' | 'user';
  };
};

const zeroAtom = new Atom<Zero<Schema>>();
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

authAtom.onChange((auth) => {
  zeroAtom.value?.close();
  mark('creating new zero');
  const z = new Zero({
    logLevel: 'info',
    server: process.env.EXPO_PUBLIC_SERVER,
    // NOTE: This is where we pass in the new expo store
    kvStore: createReplicacheExpoSQLiteKVStore,
    userID: auth?.decoded?.sub ?? 'anon',
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

export function preload(z: Zero<Schema>) {
  if (didPreload) {
    return;
  }

  didPreload = true;

  const baseIssueQuery = z.query.issue
    .related('labels')
    .related('viewState', (q) => q.where('userID', z.userID));

  const { cleanup, complete } = baseIssueQuery.preload();
  complete.then(() => {
    mark('preload complete');
    cleanup();
    baseIssueQuery
      .related('creator')
      .related('assignee')
      .related('emoji', (emoji) => emoji.related('creator'))
      .related('comments', (comments) =>
        comments
          .related('creator')
          .related('emoji', (emoji) => emoji.related('creator'))
          .limit(INITIAL_COMMENT_LIMIT) // TODO: fix
          .orderBy('created', 'desc')
      )
      .preload();
  });

  z.query.user.preload();
  z.query.label.preload();
}

// To enable accessing zero in the devtools easily.
function exposeDevHooks(z: Zero<Schema>) {
  const casted = window as unknown as {
    z?: Zero<Schema>;
  };
  casted.z = z;
}

export { authAtom as authRef, zeroAtom as zeroRef };
