import {connectFunctionsEmulator, getFunctions} from 'firebase/functions';

// This magically sets things up so that we can use the old firestore() API
// via the compatibility layer. We use the namespaced API so that we can share
// more code with the server-side logic (e.g. mirror-schema, testing mocks, etc.).
//
// https://firebase.google.com/docs/web/modular-upgrade
import firebase from 'firebase/compat/app';
import 'firebase/compat/firestore';

export function getFirebaseConfig(stack: string) {
  return stack === 'prod'
    ? {
        apiKey: 'AIzaSyCHJ8PpJjH5eQnp9sCV1xUIz7hv55OOHpQ',
        authDomain: 'reflect-mirror-prod.firebaseapp.com',
        projectId: 'reflect-mirror-prod',
        storageBucket: 'reflect-mirror-prod.appspot.com',
        messagingSenderId: '246973677105',
        appId: '1:246973677105:web:a0778898a794e54954df10',
        measurementId: 'G-MB8H4WDB5L',
      }
    : {
        apiKey: 'AIzaSyDxHw3_wWcLkpjWgprfEPhrppFr3SgV03M',
        authDomain: 'reflect-mirror-staging.firebaseapp.com',
        projectId: 'reflect-mirror-staging',
        storageBucket: 'reflect-mirror-staging.appspot.com',
        messagingSenderId: '709901628211',
        appId: '1:709901628211:web:8164d4a5cd28226a180446',
      };
}

export function initFirebase(stack: string) {
  const firebaseConfig = getFirebaseConfig(stack);

  firebase.default.initializeApp(firebaseConfig);

  if (stack === 'local') {
    connectFunctionsEmulator(getFunctions(), '127.0.0.1', 5001);
  }
}

export type Firestore = firebase.default.firestore.Firestore;

export function getFirestore(): Firestore {
  return firebase.default.firestore();
}

// Wraps a command handler with cleanup code (e.g. terminating any Firestore client)
// to ensure that the process exits after the handler completes.
export function handleWith<T>(handler: (args: T) => Promise<void>) {
  return {
    andCleanup: () => async (args: T) => {
      try {
        await handler(args);
      } finally {
        await getFirestore().terminate();
      }
    },
  };
}
