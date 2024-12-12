import {
  compactDecrypt,
  decodeProtectedHeader,
  importJWK,
  importPKCS8,
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

  if (config.verifyKey !== undefined) {
    return verifyTokenImpl(
      token,
      await loadKey(config.verifyKey, config.verifyAlgorithm, true),
      config.decryptionKey,
      verifyOptions,
    );
  }

  // jwk fetching
  throw new Error('jwkUrl is not implemented yet');
}

function loadKey(
  keyString: string,
  alg: string | undefined,
  isPublic: boolean,
) {
  try {
    const maybeVerifyKey = JSON.parse(keyString);
    if (maybeVerifyKey.kty) {
      return maybeVerifyKey as JWK;
    }
  } catch (_e) {
    // ignoring. Try as pem or symmetric key next.
  }

  // Not a JWK? Maybe it is a PEM or symmetric key
  if (keyString.startsWith('-----BEGIN')) {
    if (isPublic) {
      return importSPKI(
        keyString,
        must(
          alg,
          'verifyAlgorithm must be set when using a public key as the `verifyKey`',
        ),
      );
    }
    return importPKCS8(keyString, must(alg));
  }

  // Last shot, try as a symmetric key
  assert(
    alg === undefined,
    'Cannot set `verifyAlgorithm` when using a symmetric key as the `verifyKey`',
  );
  return new TextEncoder().encode(keyString);
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
 * - Checks various claims are present and set to the right values when `verifyOptions` is set
 * - Verifies and decrypts with either a public key or a secret key
 * - Supports https://datatracker.ietf.org/doc/html/rfc7517 JSON Web Keys too
 */
async function verifyTokenImpl(
  token: string,
  verifyKey: Uint8Array | KeyLike | JWK,
  decryptionKey: string | undefined,
  verifyOptions: JWTClaimVerificationOptions,
): Promise<JWTPayload> {
  const header = await decodeProtectedHeader(token);

  if (header.enc !== undefined) {
    assert(decryptionKey, 'Decryption key is required for encrypted JWTs');
    let loadedKey = await loadKey(decryptionKey, header.enc, false);
    if (typeof loadedKey === 'object' && 'kty' in loadedKey) {
      loadedKey = await importJWK(loadedKey);
    }
    token = await decrypt(token, loadedKey as KeyLike | Uint8Array);
  }

  const {payload} = await jwtVerify(token, verifyKey, verifyOptions);

  return payload;
}

async function decrypt(token: string, decryptionKey: KeyLike | Uint8Array) {
  const {plaintext} = await compactDecrypt(token, decryptionKey);
  return new TextDecoder().decode(plaintext);
}
