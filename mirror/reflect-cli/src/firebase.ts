import {initializeApp} from 'firebase/app';
import {connectFunctionsEmulator, getFunctions} from 'firebase/functions';
import {
  getFirestore,
  connectFirestoreEmulator,
  terminate,
} from 'firebase/firestore';
import {
  sendAnalyticsEvent,
  getUserParameters,
} from './metrics/send-ga-event.js';
import color from 'picocolors';
import type {ArgumentsCamelCase} from 'yargs';
import {reportError, ErrorInfo, Severity} from 'mirror-protocol/src/error.js';
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

  initializeApp(firebaseConfig);

  if (local) {
    connectFunctionsEmulator(getFunctions(), '127.0.0.1', 5001);
    // Note: Make sure this is different from the port that mirror-server uses,
    // as unit tests from the two packages will otherwise interfere with each other.
    connectFirestoreEmulator(getFirestore(), '127.0.0.1', 8081);
  }
}

async function reportE(
  args: ArgumentsCamelCase<CommonYargsOptions>,
  eventName: string,
  e: unknown,
  severity: Severity,
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
    severity,
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
      let success = false;
      const eventName =
        args._ && args._.length ? `cmd_${args._[0]}` : 'cmd_unknown';
      try {
        await handler(args);
        success = true;
      } catch (e) {
        await reportE(args, eventName, e, 'ERROR');
        const message = e instanceof Error ? e.message : String(e);
        console.error(`\n${color.red(color.bold('Error'))}: ${message}`);
      } finally {
        await terminate(getFirestore());
      }

      // It is tempting to send analytics in parallel with running
      // the handler, but that appears to cause problems for some commands
      // for reasons unknown.
      // https://github.com/rocicorp/mono/issues/1078
      try {
        await sendAnalyticsEvent(eventName);
      } catch (e) {
        await reportE(args, eventName, e, 'WARNING');
      }

      if (!success) {
        process.exit(-1);
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
