import {Zero} from '@rocicorp/zero';
import {Atom} from './atom.js';
import {type Schema, schema} from '../schema.js';
import {clearJwt, getJwt, getRawJwt} from './jwt.js';
import {mark} from './perf-log.js';
import {INITIAL_COMMENT_LIMIT} from './pages/issue/issue-page.js';

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

authAtom.onChange(auth => {
  zeroAtom.value?.close();
  mark('creating new zero');
  const z = new Zero({
    logLevel: 'info',
    server: import.meta.env.VITE_PUBLIC_SERVER,
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
    .related('creator')
    .related('assignee')
    .related('labels')
    .related('viewState', q => q.where('userID', z.userID).one())
    .related('emoji');

  const {cleanup, complete} = baseIssueQuery.preload();
  complete.then(() => {
    mark('preload complete');
    cleanup();
    baseIssueQuery
      .related('comments', comments =>
        comments
          .related('creator', creator => creator.one())
          .related('emoji', emoji =>
            emoji.related('creator', creator => creator.one()),
          )
          .limit(INITIAL_COMMENT_LIMIT)
          .orderBy('created', 'desc'),
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

export {authAtom as authRef, zeroAtom as zeroRef};
