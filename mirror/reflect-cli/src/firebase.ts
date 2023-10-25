import {initializeApp} from 'firebase/app';
import {connectFirestoreEmulator, getFirestore} from 'firebase/firestore';
import {connectFunctionsEmulator, getFunctions} from 'firebase/functions';

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
