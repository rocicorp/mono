import {
  jwtVerify,
  type JWK,
  type JWTClaimVerificationOptions,
  type JWTPayload,
  type KeyLike,
} from 'jose';
import {assert} from '../../../shared/src/asserts.js';
import type {AuthConfig} from '../config/zero-config.js';

export function authIsConfigured(config: AuthConfig) {
  return (
    config.jwk !== undefined ||
    config.jwksUrl !== undefined ||
    config.secret !== undefined
  );
}

export function verifyToken(
  config: AuthConfig,
  token: string,
  verifyOptions: JWTClaimVerificationOptions,
): Promise<JWTPayload> {
  verifyConfig(config);

  if (config.jwk !== undefined) {
    return verifyTokenImpl(token, loadJwk(config.jwk), verifyOptions);
  }

  if (config.secret !== undefined) {
    return verifyTokenImpl(token, loadSecret(config.secret), verifyOptions);
  }

  // jwk fetching
  throw new Error('jwksUrl is not implemented yet');
}

function loadJwk(jwkString: string) {
  return JSON.parse(jwkString) as JWK;
}

function loadSecret(secret: string) {
  return new TextEncoder().encode(secret);
}

function verifyConfig(config: AuthConfig) {
  assert(authIsConfigured(config), 'No auth options are configured.');
}

async function verifyTokenImpl(
  token: string,
  verifyKey: Uint8Array | KeyLike | JWK,
  verifyOptions: JWTClaimVerificationOptions,
): Promise<JWTPayload> {
  const {payload} = await jwtVerify(token, verifyKey, verifyOptions);

  return payload;
}
