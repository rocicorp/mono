import {initializeApp} from 'firebase/app';
import {GithubAuthProvider, getAuth} from 'firebase/auth';
import type {User as FirebaseUser} from 'firebase/auth';
import type {auth as firebaseUiAuth} from 'firebaseui';
import {ensureUser} from 'mirror-protocol/src/user.js';
import {firebaseConfig} from './firebase.config';
import * as v from 'shared/src/valita.js';
/**
 * Authentication
 */
export type AuthResult = {
  user: FirebaseUser;
};

export const firebase = initializeApp(firebaseConfig);

const githubAuthProvider = new GithubAuthProvider();

export const callbackQueryParamsSchema = v.object({
  customToken: v.string(),
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
  const userID = authResult.user.uid;
  const ensuredUser = await ensureUser({
    requester: {
      userID,
      userAgent: {
        type: 'web',
        version: '0.0.1',
      },
    },
  });

  const callbackUrl = createCallbackUrl(
    'http://localhost:8976/oauth/callback',
    ensuredUser,
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
