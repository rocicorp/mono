import {initializeApp} from 'firebase/app';
import {GithubAuthProvider, getAuth} from 'firebase/auth';
import type {auth as firebaseUiAuth} from 'firebaseui';
import {firebaseConfig} from './firebase.config';

const firebase = initializeApp(firebaseConfig);

const githubAuthProvider = new GithubAuthProvider();

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function handleAuth(authResult: any) {
  try {
    const {refreshToken, expiresIn} = authResult.user;
    const idToken = await authResult.user.getIdToken();
    const callbackUrl = new URL('http://localhost:8976/oauth/callback');
    callbackUrl.searchParams.set('idToken', idToken);
    callbackUrl.searchParams.set('refreshToken', refreshToken);
    callbackUrl.searchParams.set('expiresIn', expiresIn);

    const response = await fetch(callbackUrl);
    if (!response.ok) {
      throw new Error('Fetch error');
    }
    const data = await response.json();
    console.log('Success:', data);
  } catch (error) {
    console.error('Error:', error);
  }
}

export const uiConfig: firebaseUiAuth.Config = {
  signInOptions: [githubAuthProvider.providerId],
  signInFlow: 'popup',
  signInSuccessUrl: '/reflect-auth-welcome',
  callbacks: {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    signInSuccessWithAuthResult: (authResult: any) => {
      void handleAuth(authResult);
      return true;
    },
  },
};

export const auth = getAuth();
export default firebase;
