import {initializeApp} from 'firebase/app';
import {EmailAuthProvider, GithubAuthProvider, getAuth} from 'firebase/auth';
import type {auth as firebaseUiAuth} from 'firebaseui';
import {firebaseConfig} from './firebaseApp.config';

const firebase = initializeApp(firebaseConfig);

const githubAuthProvider = new GithubAuthProvider();
const emailAuthProvider = new EmailAuthProvider();

export const uiConfig: firebaseUiAuth.Config = {
  signInOptions: [githubAuthProvider.providerId, emailAuthProvider.providerId],
  signInFlow: 'popup',
  signInSuccessUrl: 'localhost:3000',
};

export const auth = getAuth();
export default firebase;
