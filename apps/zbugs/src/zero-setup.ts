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
let didPreload = false;

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
    kvStore: 'mem',
  });
  zeroRef.value = z;
  didPreload = false;

  // To enable accessing zero in the devtools easily.
  (window as {z?: Zero<Schema>}).z = z;
});

export function preload(z: Zero<Schema>) {
  if (didPreload) {
    return;
  }

  didPreload = true;

  z.query.user.preload();
  z.query.label.preload();

  const baseIssueQuery = z.query.issue.related('creator').related('labels');

  const {cleanup, complete} = baseIssueQuery.preload();
  complete.then(() => {
    mark('preload complete');
    cleanup();
    baseIssueQuery.related('comments', q => q.limit(10)).preload();
  });
}

export {zeroRef, authRef};
