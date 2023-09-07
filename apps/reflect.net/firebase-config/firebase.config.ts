import {FirebaseApp, initializeApp} from 'firebase/app';
import {connectFunctionsEmulator, getFunctions} from 'firebase/functions';

const firebaseConfig = process.env.NEXT_PUBLIC_FIREBASE_STAGING
  ? {
      apiKey: 'AIzaSyDM0EcyI763qTKxBPyrJ7XkPDASsCpOUms',
      authDomain: 'reflect-mirror-sandbox.firebaseapp.com',
      projectId: 'reflect-mirror-sandbox',
      storageBucket: 'reflect-mirror-sandbox.appspot.com',
      messagingSenderId: '980528051836',
      appId: '1:980528051836:web:d95f535ac5bf95c6f37bf9',
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
