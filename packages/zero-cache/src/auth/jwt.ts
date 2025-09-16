/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {
  createRemoteJWKSet,
  jwtVerify,
  type JWK,
  type JWTClaimVerificationOptions,
  type JWTPayload,
  type KeyLike,
} from 'jose';
import type {AuthConfig} from '../config/zero-config.ts';
import {exportJWK, generateKeyPair} from 'jose';

export async function createJwkPair() {
  const {publicKey, privateKey} = await generateKeyPair('PS256');

  const privateJwk = await exportJWK(privateKey);
  const publicJwk = await exportJWK(publicKey);

  privateJwk.kid = 'key-2024-001';
  privateJwk.use = 'sig';
  privateJwk.alg = 'PS256';

  publicJwk.kid = privateJwk.kid;
  publicJwk.use = privateJwk.use;
  publicJwk.alg = privateJwk.alg;

  return {privateJwk, publicJwk};
}

let remoteKeyset: ReturnType<typeof createRemoteJWKSet> | undefined;
function getRemoteKeyset(jwksUrl: string) {
  if (remoteKeyset === undefined) {
    remoteKeyset = createRemoteJWKSet(new URL(jwksUrl));
  }

  return remoteKeyset;
}

export const tokenConfigOptions = (config: AuthConfig) => {
  const tokenOptions = (['jwk', 'secret', 'jwksUrl'] as const).filter(
    key => config[key] !== undefined,
  );

  return tokenOptions;
};

export async function verifyToken(
  config: AuthConfig,
  token: string,
  verifyOptions: JWTClaimVerificationOptions,
): Promise<JWTPayload> {
  if (config.jwk !== undefined) {
    return verifyTokenImpl(token, loadJwk(config.jwk), verifyOptions);
  }

  if (config.secret !== undefined) {
    return verifyTokenImpl(token, loadSecret(config.secret), verifyOptions);
  }

  if (config.jwksUrl !== undefined) {
    const remoteKeyset = getRemoteKeyset(config.jwksUrl);
    return (await jwtVerify(token, remoteKeyset, verifyOptions)).payload;
  }

  throw new Error(
    'verifyToken was called but no auth options (one of: jwk, secret, jwksUrl) were configured.',
  );
}

function loadJwk(jwkString: string) {
  return JSON.parse(jwkString) as JWK;
}

function loadSecret(secret: string) {
  return new TextEncoder().encode(secret);
}

async function verifyTokenImpl(
  token: string,
  verifyKey: Uint8Array | KeyLike | JWK,
  verifyOptions: JWTClaimVerificationOptions,
): Promise<JWTPayload> {
  const {payload} = await jwtVerify(token, verifyKey, verifyOptions);

  return payload;
}
