import {initializeApp} from 'firebase/app';
import {getFunctions, connectFunctionsEmulator} from 'firebase/functions';

export function initFirebase(stack: string) {
  const firebaseConfig =
    stack === 'prod'
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

  initializeApp(firebaseConfig);

  if (stack === 'local') {
    connectFunctionsEmulator(getFunctions(), '127.0.0.1', 5001);
  }
}
