import {
  callbackQueryParamsSchema,
  createCallbackUrl,
} from '@/firebase-config/firebase-auth-ui-config';
import 'firebase/auth';
import jwtDecode from 'jwt-decode';
import {ensureUserResponseSchema} from 'mirror-protocol/src/user.js';
import type {GetServerSideProps} from 'next/types';
import {callFirebase} from 'shared/src/mirror/call-firebase.js';
import {assert} from 'shared/src/valita';
import * as v from 'shared/src/valita.js';

export const reflectAuthResultSchema = v.object({
  idToken: v.string(),
  refreshToken: v.string(),
  expirationTime: v.number(),
});

export type ReflectAuthResult = v.Infer<typeof reflectAuthResultSchema>;

export const authJwtTokenDecodedSchema = v.object({
  /* eslint-disable @typescript-eslint/naming-convention */
  user_id: v.string(),
  name: v.string(),
  picture: v.string(),
  iss: v.string(),
  aud: v.string(),
  auth_time: v.number(),
  sub: v.string(),
  iat: v.number(),
  exp: v.number(),
  email: v.string(),
  email_verified: v.boolean(),
  firebase: v.object({
    identities: v.object({
      'email': v.array(v.string()),
      'github.com': v.array(v.string()),
    }),
    sign_in_provider: v.string(),
  }),
  /* eslint-enable @typescript-eslint/naming-convention */
});

export type AuthJwtTokenDecoded = v.Infer<typeof authJwtTokenDecodedSchema>;

function createCliCallbackUrl(reflectAuth: ReflectAuthResult): string {
  const {idToken, refreshToken, expirationTime} = reflectAuth;
  return createCallbackUrl('http://localhost:8976/oauth/callback', {
    refreshToken,
    expirationTime: expirationTime.toString(),
    idToken,
  });
}

async function ensureUser(reflectAuth: ReflectAuthResult): Promise<boolean> {
  const token = jwtDecode<AuthJwtTokenDecoded>(reflectAuth.idToken);
  assert(token, authJwtTokenDecodedSchema);
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
  assert(authResult, callbackQueryParamsSchema);
  const {idToken, refreshToken, expirationTime} = authResult;
  const reflectAuth = {
    idToken,
    refreshToken,
    expirationTime: parseInt(expirationTime),
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

// There currently is no page to display as everything is currently being done serverside.
// This will eventually display errors and various other messages to the user.
export default function AuthCallback() {
  return;
}
