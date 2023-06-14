import * as firebase from 'firebase/app';
import {EmailAuthProvider, getAuth} from 'firebase/auth';
import {GithubAuthProvider} from 'firebase/auth';
import type {auth as firebaseUiAuth} from 'firebaseui';

import {firebaseConfig} from '@/config/firebaseApp.config';

// eslint-disable-next-line @typescript-eslint/naming-convention
const Firebase = firebase.initializeApp(firebaseConfig);

// Add or Remove authentification methods here.
// eslint-disable-next-line @typescript-eslint/naming-convention
export const Providers = {
  github: new GithubAuthProvider(),
  email: new EmailAuthProvider(),
};

export const uiConfig: firebaseUiAuth.Config = {
  signInOptions: [Providers.github.providerId, Providers.email.providerId],
  signInFlow: 'popup',
  signInSuccessUrl: 'localhost:3000',
};

export const auth = getAuth();
export default Firebase;
