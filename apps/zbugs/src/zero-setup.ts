import {Ref, Zero} from '@rocicorp/zero';
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

const zeroRef = new Ref<Zero<Schema>>();
const authRef = new Ref<LoginState>();
const jwt = getJwt();
const encodedJwt = getRawJwt();

authRef.set(
  encodedJwt && jwt
    ? {
        encoded: encodedJwt,
        decoded: jwt as LoginState['decoded'],
      }
    : undefined,
);

authRef.onChange(auth => {
  zeroRef.get()?.close();
  mark('creating new zero');
  const z = new Zero({
    logLevel: 'info',
    server: import.meta.env.VITE_PUBLIC_SERVER,
    userID: auth?.decoded?.sub ?? 'anon',
    auth: auth?.encoded,
    schema,
  });
  zeroRef.set(z);

  // To enable accessing zero in the devtools easily.
  (window as {z?: Zero<Schema>}).z = z;

  const baseIssueQuery = z.query.issue.related('creator').related('labels');

  const {cleanup, complete} = baseIssueQuery.preload();
  complete.then(() => {
    mark('preload complete');
    cleanup();
    baseIssueQuery.related('comments', q => q.limit(10)).preload();
  });

  z.query.user.preload();
  z.query.label.preload();
});

export {zeroRef, authRef};
