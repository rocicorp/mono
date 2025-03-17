import {SignJWT, type JWTPayload} from 'jose';
import {describe, expect, test} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import type {AuthConfig} from '../config/zero-config.ts';
import {createJwkPair, verifyToken} from './jwt.ts';

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

test('too many or too few options set', async () => {
  await expect(verifyToken({}, '', {})).rejects.toThrowError('Exactly one of');
  await expect(
    verifyToken(
      {
        secret: 'abc',
        jwk: 'def',
      },
      '',
      {},
    ),
  ).rejects.toThrowError('Exactly one of');
  await expect(
    verifyToken(
      {
        secret: 'abc',
        jwksUrl: 'def',
      },
      '',
      {},
    ),
  ).rejects.toThrowError('Exactly one of');
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
}
