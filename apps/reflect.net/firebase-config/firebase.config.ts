import {FirebaseApp, initializeApp} from 'firebase/app';
import {connectFunctionsEmulator, getFunctions} from 'firebase/functions';

const firebaseConfig = process.env.NEXT_PUBLIC_FIREBASE_STAGING
  ? {
      apiKey: 'AIzaSyDxHw3_wWcLkpjWgprfEPhrppFr3SgV03M',
      authDomain: 'reflect-mirror-staging.firebaseapp.com',
      projectId: 'reflect-mirror-staging',
      storageBucket: 'reflect-mirror-staging.appspot.com',
      messagingSenderId: '709901628211',
      appId: '1:709901628211:web:8164d4a5cd28226a180446',
    }
  : {
      apiKey: 'AIzaSyCHJ8PpJjH5eQnp9sCV1xUIz7hv55OOHpQ',
      authDomain: 'reflect-mirror-prod.firebaseapp.com',
      projectId: 'reflect-mirror-prod',
      storageBucket: 'reflect-mirror-prod.appspot.com',
      messagingSenderId: '246973677105',
      appId: '1:246973677105:web:a0778898a794e54954df10',
      measurementId: 'G-MB8H4WDB5L',
    };

let firebase: FirebaseApp | undefined = undefined;

/** Must be called before using Firebase client libraries. */
export function initFirebaseApp() {
  if (!firebase) {
    firebase = initializeApp(firebaseConfig);
    if (process.env.NEXT_PUBLIC_USE_FUNCTIONS_EMULATOR) {
      connectFunctionsEmulator(getFunctions(), '127.0.0.1', 5001);
    }
  }
  return firebase;
}
