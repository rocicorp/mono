import {and, cmp, not, or, Zero} from '@rocicorp/zero';
import {Atom} from './atom.js';
import {type Schema, schema} from './domain/schema.js';
import {getJwt, getRawJwt} from './jwt.js';
import {mark} from './perf-log.js';

export type LoginState = {
  encoded: string;
  decoded: {
    sub: string;
    name: string;
  };
};

const zeroRef = new Atom<Zero<Schema>>();
const authRef = new Atom<LoginState>();
const jwt = getJwt();
const encodedJwt = getRawJwt();

authRef.value =
  encodedJwt && jwt
    ? {
        encoded: encodedJwt,
        decoded: jwt as LoginState['decoded'],
      }
    : undefined;

authRef.onChange(auth => {
  zeroRef.value?.close();
  mark('creating new zero');
  const z = new Zero({
    logLevel: 'info',
    server: import.meta.env.VITE_PUBLIC_SERVER,
    userID: auth?.decoded?.sub ?? 'anon',
    auth: auth?.encoded,
    schema,
  });
  zeroRef.value = z;

  exposeDevHooks(z);

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
      .related('comments', q => q.related('emoji').limit(10))
      .preload();
  });

  z.query.user.preload();
  z.query.label.preload();
});

// To enable accessing zero in the devtools easily.
function exposeDevHooks(z: Zero<Schema>) {
  const casted = window as unknown as {
    z?: Zero<Schema>;
    or: typeof or;
    and: typeof and;
    not: typeof not;
    cmp: typeof cmp;
  };
  casted.z = z;
  casted.or = or;
  casted.and = and;
  casted.not = not;
  casted.cmp = cmp;
}

export {authRef, zeroRef};
