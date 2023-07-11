import 'firebase/auth';
import jwtDecode from 'jwt-decode';
import {ensureUserResponseSchema} from 'mirror-protocol/src/user.js';
import type {GetServerSideProps} from 'next/types';
import {callFirebase} from 'shared/src/mirror/call-firebase.js';

export type ReflectAuthResult = {
  idToken: string;
  refreshToken: string;
  expirationTime: number;
};

function createCliCallbackUrl(reflectAuth: ReflectAuthResult): string {
  const {idToken, refreshToken, expirationTime} = reflectAuth;
  const callbackUrl = new URL('http://localhost:8976/oauth/callback');
  callbackUrl.searchParams.set('idToken', idToken);
  callbackUrl.searchParams.set('refreshToken', refreshToken);
  callbackUrl.searchParams.set('expirationTime', expirationTime.toString());
  return callbackUrl.toString();
}

async function ensureUser(reflectAuth: ReflectAuthResult): Promise<boolean> {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const token = jwtDecode<{user_id: string}>(reflectAuth.idToken);
  const data = {
    requester: {
      userID: token.user_id,
      userAgent: {
        type: 'web',
        version: '0.0.1',
      },
    },
  };

  //todo(cesar): probably should bubble up if an error is thrown here
  const fbResponse = await callFirebase(
    'user-ensure',
    data,
    reflectAuth.idToken,
    ensureUserResponseSchema,
  );

  return fbResponse.success;
}

export const getServerSideProps: GetServerSideProps<{
  authResult: ReflectAuthResult;
}> = async context => {
  const authResult = context.query;
  const reflectAuth = {
    idToken: authResult['idToken'] as string,
    refreshToken: authResult['refreshToken'] as string,
    expirationTime: parseInt(authResult['expirationTime'] as string),
  };
  const user = await ensureUser(reflectAuth);
  if (!user) {
    throw new Error('failed to ensure user');
  }
  const cliUrl = createCliCallbackUrl(reflectAuth);
  context.res.setHeader('Location', cliUrl);
  context.res.statusCode = 302;
  return {props: {authResult: {...reflectAuth}}};
};

export default function AuthCallback() {
  return;
}
