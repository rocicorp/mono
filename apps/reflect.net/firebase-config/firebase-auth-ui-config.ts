import {initializeApp} from 'firebase/app';
import {GithubAuthProvider, getAuth} from 'firebase/auth';
import type {auth as firebaseUiAuth} from 'firebaseui';
import {firebaseConfig} from './firebase.config';

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

export const firebase = initializeApp(firebaseConfig);

const githubAuthProvider = new GithubAuthProvider();

const handleAuth = async (authResult: AuthResult) => {
  const {refreshToken} = authResult.user;
  const {expirationTime} = authResult.user.stsTokenManager;
  const idToken = await authResult.user.getIdToken();
  const callbackUrl = `/auth-callback?refreshToken=${refreshToken}&expirationTime=${expirationTime}&idToken=${idToken}`;
  window.location.replace(callbackUrl.toString());
};
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
