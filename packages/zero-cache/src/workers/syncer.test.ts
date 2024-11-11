import {describe, expect, test} from 'vitest';
import {decodeAndCheckToken} from './syncer.js';
import {SignJWT} from 'jose';

function makeJWT(payload: Record<string, unknown> = {sub: 'testID'}) {
  return new SignJWT(payload)
    .setProtectedHeader({alg: 'HS256'})
    .setExpirationTime('30days')
    .sign(new TextEncoder().encode('testing-secret'));
}

describe('decodeAndCheckToken', () => {
  test('no secret', async () => {
    await expect(async () =>
      decodeAndCheckToken(await makeJWT(), undefined, 'testID'),
    ).rejects.toThrow('JWT secret was not set in `zero.config`');
  });

  test('invalid token', async () => {
    await expect(() =>
      decodeAndCheckToken(
        'invalid',
        new TextEncoder().encode('testing-secret'),
        'testID',
      ),
    ).rejects.toThrow();
  });

  test('valid token', async () => {
    const jwt = await makeJWT();
    const decoded = await decodeAndCheckToken(
      jwt,
      new TextEncoder().encode('testing-secret'),
      'testID',
    );
    expect(decoded.sub).toBe('testID');
  });

  test('userID != sub', async () => {
    const jwt = await makeJWT({sub: 'otherID'});
    await expect(() =>
      decodeAndCheckToken(
        jwt,
        new TextEncoder().encode('testing-secret'),
        'testID',
      ),
    ).rejects.toThrow('JWT subject does not match the userID');
  });
});
