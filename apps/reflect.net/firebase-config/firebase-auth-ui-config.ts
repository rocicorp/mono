import {initializeApp} from 'firebase/app';
import {GithubAuthProvider, getAuth} from 'firebase/auth';
import type {auth as firebaseUiAuth} from 'firebaseui';
import {firebaseConfig} from './firebase.config';

export const firebase = initializeApp(firebaseConfig);

const githubAuthProvider = new GithubAuthProvider();

/**
 * Authentication
 */
export type FirebaseUser = {
  getIdToken(): Promise<string>;
  displayName: string;
  email: string;
  emailVerified: boolean;
  isAnonymous: boolean;
  photoURL: string;
  refreshToken: string;
  uid: string;
  stsTokenManager: {
    accessToken: string;
    apiKey: string;
    expirationTime: number;
    refreshToken: string;
  };
};
export type AuthResult = {
  user: FirebaseUser;
};

async function handleAuth(authResult: AuthResult) {
  const {refreshToken} = authResult.user;
  const {expirationTime} = authResult.user.stsTokenManager;
  const idToken = await authResult.user.getIdToken();
  const callbackUrl = new URL('http://localhost:8976/oauth/callback');
  callbackUrl.searchParams.set('idToken', idToken);
  callbackUrl.searchParams.set('refreshToken', refreshToken);
  callbackUrl.searchParams.set('expirationTime', expirationTime.toString());
  //browser navigate to callbackUrl
  window.location.replace(callbackUrl.toString());
}

export const uiConfig: firebaseUiAuth.Config = {
  signInOptions: [githubAuthProvider.providerId],
  signInFlow: 'popup',
  callbacks: {
    signInSuccessWithAuthResult: authResult => {
      void handleAuth(authResult);
      return false;
    },
  },
};

export const auth = getAuth();
