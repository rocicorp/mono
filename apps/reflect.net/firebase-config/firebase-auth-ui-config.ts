import type {AuthCredential, User as FirebaseUser} from 'firebase/auth';
import {GithubAuthProvider, getAuth} from 'firebase/auth';
import type {auth as firebaseUiAuth} from 'firebaseui';
import {ensureUser} from 'mirror-protocol/src/user.js';
import * as v from 'shared/src/valita.js';
import {initFirebaseApp} from './firebase.config';
/**
 * https://github.com/firebase/firebaseui-web/blob/de8a5b0f26cf87b1637c6a5f40c45278aba2945e/javascript/widgets/config.js#L922
 */
export type AuthResult = {
  user: FirebaseUser;
  credential: AuthCredential;
};

initFirebaseApp();

// Ping the `user-ensure` function to avoid a cold-start when the user actually logs in.
ensureUser.warm();

const githubAuthProvider = new GithubAuthProvider();

export const callbackQueryParamsSchema = v.object({
  authCredential: v.string(),
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
  await ensureUser.call({
    requester: {
      userID,
      userAgent: {
        type: 'web',
        version: '0.0.1',
      },
    },
  });

  const {credential} = authResult;
  const callbackUrl = createCallbackUrl(
    'http://localhost:8976/oauth/callback',
    {authCredential: JSON.stringify(credential.toJSON())},
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
