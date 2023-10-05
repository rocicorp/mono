import {connectFunctionsEmulator, getFunctions} from 'firebase/functions';

// This magically sets things up so that we can use the old firestore() API
// via the compatibility layer. We use the namespaced API so that we can share
// more code with the server-side logic (e.g. mirror-schema, testing mocks, etc.).
//
// https://firebase.google.com/docs/web/modular-upgrade
import firebase from 'firebase/compat/app';
import 'firebase/compat/firestore';
import {
  sendAnalyticsEvent,
  getUserParameters,
} from './metrics/send-ga-event.js';
import type {ArgumentsCamelCase} from 'yargs';
import {reportError, ErrorInfo} from 'mirror-protocol/src/error.js';
import {version} from './version.js';
import {getAuthentication} from './auth-config.js';
import type {CommonYargsOptions} from './yarg-types.js';
function getFirebaseConfig(stack: string) {
  switch (stack) {
    case 'sandbox':
      return {
        apiKey: 'AIzaSyDM0EcyI763qTKxBPyrJ7XkPDASsCpOUms',
        authDomain: 'reflect-mirror-sandbox.firebaseapp.com',
        projectId: 'reflect-mirror-sandbox',
        storageBucket: 'reflect-mirror-sandbox.appspot.com',
        messagingSenderId: '980528051836',
        appId: '1:980528051836:web:57bbf00f13e35bd9f37bf9',
      };
    case 'staging': // TODO(darick): Remove this once cleanup is complete.
      return {
        apiKey: 'AIzaSyDxHw3_wWcLkpjWgprfEPhrppFr3SgV03M',
        authDomain: 'reflect-mirror-staging.firebaseapp.com',
        projectId: 'reflect-mirror-staging',
        storageBucket: 'reflect-mirror-staging.appspot.com',
        messagingSenderId: '709901628211',
        appId: '1:709901628211:web:8164d4a5cd28226a180446',
      };
    default: // 'prod'
      return {
        apiKey: 'AIzaSyCHJ8PpJjH5eQnp9sCV1xUIz7hv55OOHpQ',
        authDomain: 'reflect-mirror-prod.firebaseapp.com',
        projectId: 'reflect-mirror-prod',
        storageBucket: 'reflect-mirror-prod.appspot.com',
        messagingSenderId: '246973677105',
        appId: '1:246973677105:web:a0778898a794e54954df10',
        measurementId: 'G-MB8H4WDB5L',
      };
  }
}

export function initFirebase(args: {stack: string; local: boolean}) {
  const {stack, local} = args;
  const firebaseConfig = getFirebaseConfig(stack);

  firebase.default.initializeApp(firebaseConfig);

  if (local) {
    connectFunctionsEmulator(getFunctions(), '127.0.0.1', 5001);
  }
}

export type Firestore = firebase.default.firestore.Firestore;

export function getFirestore(): Firestore {
  return firebase.default.firestore();
}

async function reportE(
  args: ArgumentsCamelCase<CommonYargsOptions>,
  eventName: string,
  e: unknown,
) {
  let userID = '';
  try {
    ({userID} = await getAuthentication(args));
  } catch (e) {
    /* swallow */
  }
  await reportError({
    action: eventName,
    error: createErrorInfo(e),
    requester: {
      userID,
      userAgent: {type: 'reflect-cli', version},
    },
    agentContext: getUserParameters(version),
  }).catch(_err => {
    /* swallow */
  });
}

// Wraps a command handler with cleanup code (e.g. terminating any Firestore client)
// to ensure that the process exits after the handler completes.
export function handleWith<T extends ArgumentsCamelCase<CommonYargsOptions>>(
  handler: (args: T) => Promise<void>,
) {
  return {
    andCleanup: () => async (args: T) => {
      const eventName =
        args._ && args._.length ? `cmd_${args._[0]}` : 'cmd_unknown';
      try {
        await Promise.all([
          sendAnalyticsEvent(eventName).catch(async e => {
            await reportE(args, eventName, e);
          }),
          handler(args),
        ]);
      } catch (e) {
        await reportE(args, eventName, e);
        throw e;
      } finally {
        await getFirestore().terminate();
      }
    },
  };
}

function createErrorInfo(e: unknown): ErrorInfo {
  if (!(e instanceof Error)) {
    return {desc: String(e)};
  }
  return {
    desc: String(e),
    name: e.name,
    message: e.message,
    stack: e.stack,
    cause: createErrorInfo(e.cause),
  };
}
