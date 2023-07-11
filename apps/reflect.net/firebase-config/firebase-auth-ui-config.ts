import {initializeApp} from 'firebase/app';
import {GithubAuthProvider, getAuth} from 'firebase/auth';
import type {auth as firebaseUiAuth} from 'firebaseui';
import {firebaseConfig} from './firebase.config';
import * as v from 'shared/src/valita.js';
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

export const callbackQueryParamsSchema = v.object({
  idToken: v.string(),
  refreshToken: v.string(),
  expirationTime: v.string(),
});

export type CallbackQueryParams = v.Infer<typeof callbackQueryParamsSchema>;

export const createCallbackUrl = (
  callbackBaseUrl: string,
  queryParams: CallbackQueryParams,
  locationHref?: string | undefined,
) => {
  const callbackUrl = new URL(callbackBaseUrl, locationHref);
  Object.entries(queryParams).forEach(([key, value]) => {
    callbackUrl.searchParams.set(key, value);
  });
  return callbackUrl.toString();
};

const handleAuth = async (authResult: AuthResult) => {
  const {refreshToken} = authResult.user;
  const {expirationTime} = authResult.user.stsTokenManager;
  const idToken = await authResult.user.getIdToken();

  const callbackUrl = createCallbackUrl(
    '/auth-callback',
    {
      refreshToken,
      expirationTime: expirationTime.toString(),
      idToken,
    },
    location.href,
  );

  window.location.replace(callbackUrl);
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
