import {initializeApp} from 'firebase/app';
import {EmailAuthProvider, GithubAuthProvider, getAuth} from 'firebase/auth';
import type {auth as firebaseUiAuth} from 'firebaseui';
import {firebaseConfig} from './firebaseApp.config';
import type {redirect} from 'next/dist/server/api-utils';

const firebase = initializeApp(firebaseConfig);

const githubAuthProvider = new GithubAuthProvider();
const emailAuthProvider = new EmailAuthProvider();

export const uiConfig: firebaseUiAuth.Config = {
  signInOptions: [githubAuthProvider.providerId, emailAuthProvider.providerId],
  signInFlow: 'popup',
  signInSuccessUrl: '/reflect-auth-welcome',
  callbacks: {
    signInSuccessWithAuthResult: authResult => {
      const {refreshToken, expiresIn} = authResult.user;
      authResult.user.getIdToken().then((idToken: string) => {
        const callbackUrl = new URL('http://localhost:8976/oauth/callback');
        callbackUrl.searchParams.set('idToken', idToken);
        callbackUrl.searchParams.set('refreshToken', refreshToken);
        callbackUrl.searchParams.set('expiresIn', expiresIn);
        fetch(callbackUrl, {
          method: 'GET',
        })
          .then(response => response.json())
          .then(data => {
            console.log('Success:', data);
          })
          .catch(error => {
            console.error('Error:', error);
          });
      });
      return true;
    },
  },
};

export const auth = getAuth();
export default firebase;
