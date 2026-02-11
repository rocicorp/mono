import {SignJWT, type JWTPayload} from 'jose';
import {describe, expect, test} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import type {AuthConfig} from '../config/zero-config.ts';
import {createJwkPair, tokenConfigOptions, verifyToken} from './jwt.ts';

describe('symmetric key', () => {
  const key = 'ab'.repeat(16);
  async function makeToken(tokenData: JWTPayload) {
    const token = await new SignJWT(tokenData)
      .setProtectedHeader({alg: 'HS256'})
      .sign(new TextEncoder().encode(key));
    return {expected: tokenData, token};
  }

  commonTests({secret: key}, makeToken);
});

describe('jwk', async () => {
  const {privateJwk, publicJwk} = await createJwkPair();
  async function makeToken(tokenData: JWTPayload) {
    const token = await new SignJWT(tokenData)
      .setProtectedHeader({
        alg: must(privateJwk.alg),
      })
      .sign(privateJwk);
    return {expected: tokenData, token};
  }

  commonTests({jwk: JSON.stringify(publicJwk)}, makeToken);
});

test('token config options', async () => {
  await expect(tokenConfigOptions({})).toEqual([]);
  await expect(tokenConfigOptions({secret: 'abc'})).toEqual(['secret']);
  await expect(tokenConfigOptions({jwk: 'def'})).toEqual(['jwk']);
  await expect(tokenConfigOptions({jwksUrl: 'ghi'})).toEqual(['jwksUrl']);
  await expect(tokenConfigOptions({jwksUrl: 'jkl', secret: 'mno'})).toEqual([
    'secret',
    'jwksUrl',
  ]);
});

test('no options set', async () => {
  await expect(verifyToken({}, '', {})).rejects.toThrowError(
    'verifyToken was called but no auth options',
  );
});

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
      `Invalid Compact JWS`,
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

  test('valid issuer', async () => {
    const {expected, token} = await makeToken({
      sub: '123',
      iss: 'https://issuer.example.com',
      exp: Math.floor(Date.now() / 1000) + 100,
    });
    expect(
      await verifyToken(config, token, {issuer: 'https://issuer.example.com'}),
    ).toEqual(expected);
  });

  test('invalid audience', async () => {
    const {token} = await makeToken({
      sub: '123',
      aud: 'app-123',
    });
    await expect(() =>
      verifyToken(config, token, {audience: 'app-456'}),
    ).rejects.toThrowError(`unexpected "aud" claim value`);
  });

  test('valid audience', async () => {
    const {expected, token} = await makeToken({
      sub: '123',
      aud: 'my-app',
      exp: Math.floor(Date.now() / 1000) + 100,
    });
    expect(await verifyToken(config, token, {audience: 'my-app'})).toEqual(
      expected,
    );
  });

  test('audience in token but not in config should pass', async () => {
    const {expected, token} = await makeToken({
      sub: '123',
      aud: 'some-audience',
      exp: Math.floor(Date.now() / 1000) + 100,
    });
    // When audience is not specified in verify options, the aud claim is not validated
    expect(await verifyToken(config, token, {})).toEqual(expected);
  });

  test('issuer in token but not in config should pass', async () => {
    const {expected, token} = await makeToken({
      sub: '123',
      iss: 'https://some-issuer.com',
      exp: Math.floor(Date.now() / 1000) + 100,
    });
    // When issuer is not specified in verify options, the iss claim is not validated
    expect(await verifyToken(config, token, {})).toEqual(expected);
  });
}
