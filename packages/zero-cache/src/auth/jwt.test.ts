import {describe, expect, test} from 'vitest';
import type {AuthConfig} from '../config/zero-config.js';
import {verifyToken} from './jwt.js';
import {
  exportJWK,
  generateKeyPair,
  importPKCS8,
  SignJWT,
  type JWTPayload,
} from 'jose';
import {generateKeyPair as crypoGenerateKeyPair} from 'node:crypto';
import {promisify} from 'node:util';
import {must} from '../../../shared/src/must.js';

const generateKeyPairAsync = promisify(crypoGenerateKeyPair);

// generate a public and private key pair
async function generateKeys() {
  const {privateKey, publicKey} = await generateKeyPairAsync('rsa', {
    modulusLength: 2048,
    publicKeyEncoding: {
      type: 'spki',
      format: 'pem',
    },
    privateKeyEncoding: {
      type: 'pkcs8',
      format: 'pem',
    },
  });

  return {privateKey, publicKey};
}

async function generateJwkKeys() {
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

describe('secret key', () => {
  const key = 'abc123';
  const makeToken = async (tokenData: JWTPayload) => {
    const token = await new SignJWT(tokenData)
      .setProtectedHeader({alg: 'HS256'})
      .sign(new TextEncoder().encode(key));
    return {expected: tokenData, token};
  };

  commonTests({verifyKey: key}, makeToken);
});

describe('public key', async () => {
  const keys = await generateKeys();

  async function makeToken(tokenData: JWTPayload) {
    const privateKey = await importPKCS8(keys.privateKey, 'RS256');
    const token = await new SignJWT(tokenData)
      .setProtectedHeader({alg: 'RS256'})
      .sign(privateKey);
    return {expected: tokenData, token};
  }

  commonTests({verifyKey: keys.publicKey, verifyAlgorithm: 'RS256'}, makeToken);
});

describe('jwk', async () => {
  const {privateJwk, publicJwk} = await generateJwkKeys();
  async function makeToken(tokenData: JWTPayload) {
    const token = await new SignJWT(tokenData)
      .setProtectedHeader({
        alg: must(privateJwk.alg),
      })
      .sign(privateJwk);
    return {expected: tokenData, token};
  }

  commonTests({verifyKey: JSON.stringify(publicJwk)}, makeToken);
});

// encrypted token
// describe('jwkUrl', () => {});
// describe('invalid config states', () => {});

function commonTests(
  config: AuthConfig,
  makeToken: (
    tokenData: JWTPayload,
  ) => Promise<{expected: JWTPayload; token: string}>,
) {
  test('valid token', async () => {
    const {expected, token} = await makeToken({
      sub: '123',
      exp: Math.floor(Date.now() / 1000) + 100,
      role: 'something',
    });
    expect(await verifyToken(config, token, {})).toEqual(expected);
  });

  test('expired token', async () => {
    const {token} = await makeToken({
      sub: '123',
      exp: Math.floor(Date.now() / 1000) - 100,
    });
    await expect(() => verifyToken(config, token, {})).rejects.toThrowError(
      `"exp" claim timestamp check failed`,
    );
  });

  test('not yet valid token', async () => {
    const {token} = await makeToken({
      sub: '123',
      nbf: Math.floor(Date.now() / 1000) + 100,
    });
    await expect(() => verifyToken(config, token, {})).rejects.toThrowError(
      `"nbf" claim timestamp check failed`,
    );
  });

  test('invalid subject', async () => {
    const {token} = await makeToken({
      sub: '123',
      nbf: Math.floor(Date.now() / 1000) + 100,
    });
    await expect(() =>
      verifyToken(config, token, {subject: '321'}),
    ).rejects.toThrowError(`unexpected "sub" claim value`);
  });

  test('invalid token', async () => {
    await expect(() => verifyToken(config, 'sdfsdf', {})).rejects.toThrowError(
      `Invalid Token or Protected Header formatting`,
    );
  });

  test('invalid issuer', async () => {
    const {token} = await makeToken({
      sub: '123',
      iss: 'abc',
    });
    await expect(() =>
      verifyToken(config, token, {issuer: 'def'}),
    ).rejects.toThrowError(`unexpected "iss" claim value`);
  });

  test('can read claims', () => {});
}
