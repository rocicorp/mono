import 'firebase/auth';
import type {GetServerSideProps, InferGetServerSidePropsType} from 'next/types';
import {useEffect} from 'react';
import jwtDecode from 'jwt-decode';
import {ensureUserResponseSchema} from 'mirror-protocol/src/user';
import {callFirebase} from 'shared/src/call-firebase';

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

async function ensureUser(reflectAuth: ReflectAuthResult): Promise<string> {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const token = jwtDecode<{user_id: string}>(reflectAuth.idToken);
  const data = {
    requester: {
      userId: token.user_id,
      userAgent: 'browser',
    },
  };

  const user = await callFirebase(
    'user-ensure',
    data,
    ensureUserResponseSchema,
    reflectAuth.idToken,
  );
  return user;
}

export const getServerSideProps: GetServerSideProps<{
  authResult: ReflectAuthResult;
}> = async context => {
  const authResult = await context.query;
  const reflectAuth = {
    idToken: authResult['idToken'] as string,
    refreshToken: authResult['refreshToken'] as string,
    expirationTime: parseInt(authResult['expirationTime'] as string),
  };
  const user = await ensureUser(reflectAuth);
  if (!user) {
    throw new Error('failed to ensure user');
  }
  return {props: {authResult: {...reflectAuth}}};
};

export default function AuthCallback({
  authResult,
}: InferGetServerSidePropsType<typeof getServerSideProps>) {
  const url = createCliCallbackUrl(authResult);
  useEffect(() => {
    window.location.replace(url);
  }, [url]);

  return authResult.idToken ? (
    <div>Redirecting you to ${} </div>
  ) : (
    <div>Something went wrong with authentication</div>
  );
}
