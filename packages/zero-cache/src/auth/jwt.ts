import {
  compactDecrypt,
  decodeProtectedHeader,
  importSPKI,
  jwtVerify,
  type JWK,
  type JWTClaimVerificationOptions,
  type JWTPayload,
  type KeyLike,
} from 'jose';
import {assert} from '../../../shared/src/asserts.js';
import type {AuthConfig} from '../config/zero-config.js';
import {must} from '../../../shared/src/must.js';

export function authIsConfigured(config: AuthConfig) {
  return config.verifyKey !== undefined || config.jwkUrl !== undefined;
}

export async function verifyToken(
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
    let verifyKey: Uint8Array | JWK | KeyLike | undefined;

    // Try for JWK first
    try {
      const maybeVerifyKey = JSON.parse(config.verifyKey);
      if (maybeVerifyKey.kty) {
        verifyKey = maybeVerifyKey as JWK;
      }
    } catch (_e) {
      // ignoring. Try as pem or symmetric key next.
    }

    // Not a JWK? Maybe it is a PEM or symmetric key
    if (verifyKey === undefined) {
      if (config.verifyKey.startsWith('-----BEGIN PUBLIC KEY')) {
        verifyKey = await importSPKI(
          config.verifyKey,
          must(
            config.verifyAlgorithm,
            'verifyAlgorithm must be set when using a public key as the `verifyKey`',
          ),
        );
      } else {
        // Last shot, try as a symmetric key
        verifyKey = new TextEncoder().encode(config.verifyKey);
      }
    }

    return verifyTokenImpl(token, verifyKey, decryptionKey, verifyOptions);
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

  if (config.verifyAlgorithm !== undefined) {
    assert(
      config.verifyKey !== undefined,
      'Cannot set `verifyAlgorithm` without also setting `verifyKey` in `zero.config`.',
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
async function verifyTokenImpl(
  token: string,
  verifyKey: Uint8Array | KeyLike | JWK,
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
