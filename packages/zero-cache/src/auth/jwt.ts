import {
  compactDecrypt,
  decodeProtectedHeader,
  jwtVerify,
  type JWK,
  type JWTClaimVerificationOptions,
  type JWTPayload,
  type KeyLike,
} from 'jose';
import {assert} from '../../../shared/src/asserts.js';
import type {AuthConfig} from '../config/zero-config.js';

export function authIsConfigured(config: AuthConfig) {
  return config.verifyKey !== undefined || config.jwkUrl !== undefined;
}

export function decryptDecodeVerifyJWT(
  config: AuthConfig,
  token: string,
  verifyOptions: JWTClaimVerificationOptions,
): Promise<JWTPayload> {
  verifyConfig(config);

  let decryptionKey: Uint8Array | undefined;
  if (config.decryptionKey !== undefined) {
    decryptionKey = new TextEncoder().encode(config.decryptionKey);
  }

  if (config.verifyKey !== undefined) {
    const verifyKey = new TextEncoder().encode(config.verifyKey);
    return decryptDecodeVerifyJWTImpl(
      token,
      verifyKey,
      decryptionKey,
      verifyOptions,
    );
  }

  // jwk fetching
  throw new Error('jwkUrl is not implemented yet');
}

function verifyConfig(config: AuthConfig) {
  assert(authIsConfigured(config), 'Auth is not configured');

  assert(
    config.jwkUrl !== undefined || config.verifyKey !== undefined,
    'Either `jwkUrl` or `verifyKey` must be set in `zero.config`',
  );

  if (
    config.decryptionKey !== undefined ||
    config.decryptionAlgorithm !== undefined
  ) {
    assert(
      config.decryptionAlgorithm !== undefined &&
        config.decryptionKey !== undefined,
      'Cannot set `decryptionKey` without also setting `decryptionAlgorithm` in `zero.config`',
    );
  }
}

/**
 * - Decrypts the JWT if it is encrypted
 * - Verifies the JWT signature
 * - Checks the expiration time
 * - Checks the sub field matches the user id if a sub field is provided
 * - Verifies and decrypts with either a public key or a secret key
 * - Supports https://datatracker.ietf.org/doc/html/rfc7517 JSON Web Keys too
 */
async function decryptDecodeVerifyJWTImpl(
  token: string,
  verifyKey: Uint8Array | JWK,
  decryptionKey: KeyLike | Uint8Array | undefined,
  verifyOptions: JWTClaimVerificationOptions,
): Promise<JWTPayload> {
  const header = await decodeProtectedHeader(token);

  if (header.enc !== undefined) {
    assert(decryptionKey, 'Decryption key is required for encrypted JWTs');
    token = await decrypt(token, decryptionKey);
  }

  const {payload} = await jwtVerify(token, verifyKey, verifyOptions);

  return payload;
}

async function decrypt(
  token: string,
  decryptionKey: KeyLike | Uint8Array | undefined,
) {
  assert(decryptionKey, 'Decryption key is required for encrypted JWTs');
  const {plaintext} = await compactDecrypt(token, decryptionKey);
  return new TextDecoder().decode(plaintext);
}
