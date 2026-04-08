import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {
  isAuthErrorBody,
  pickToken,
  resolveAuth,
  type ValidateLegacyJWT,
} from './auth.ts';

describe('pickToken', () => {
  const lc = createSilentLogContext();

  test('previous token is undefined', () => {
    expect(
      pickToken(lc, undefined, {
        type: 'jwt',
        decoded: {sub: 'foo', iat: 1},
        raw: '',
      }),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 1,
      },
      raw: '',
      type: 'jwt',
    });
  });

  test('opaque tokens when previous undefined', () => {
    expect(pickToken(lc, undefined, {type: 'opaque', raw: 'opaque-1'})).toEqual(
      {type: 'opaque', raw: 'opaque-1'},
    );
  });

  test('opaque tokens allow replacement', () => {
    expect(
      pickToken(
        lc,
        {type: 'opaque', raw: 'opaque-1'},
        {type: 'opaque', raw: 'opaque-2'},
      ),
    ).toEqual({type: 'opaque', raw: 'opaque-2'});
  });

  test('opaque token cannot replace jwt token', () => {
    expect(() =>
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo', iat: 1}, raw: ''},
        {type: 'opaque', raw: 'opaque-1'},
      ),
    ).toThrowError(ProtocolError);
  });

  test('jwt token cannot replace opaque token', () => {
    expect(() =>
      pickToken(
        lc,
        {type: 'opaque', raw: 'opaque-1'},
        {type: 'jwt', decoded: {sub: 'foo', iat: 1}, raw: ''},
      ),
    ).toThrowError(ProtocolError);
  });

  test('previous token exists, new token is undefined', () => {
    expect(() =>
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo', iat: 1}, raw: ''},
        undefined,
      ),
    ).toThrowError(ProtocolError);
  });
});

describe('resolveAuth', () => {
  const lc = createSilentLogContext();

  test('resolves opaque auth without group binding side effects', async () => {
    await expect(
      resolveAuth(lc, undefined, 'u1', 'opaque-1', undefined),
    ).resolves.toEqual({type: 'opaque', raw: 'opaque-1'});
  });

  test('reuses the existing opaque auth object when the token is unchanged', async () => {
    const existingAuth = {type: 'opaque', raw: 'opaque-1'} as const;

    await expect(
      resolveAuth(lc, existingAuth, 'u1', 'opaque-1', undefined),
    ).resolves.toBe(existingAuth);
  });

  test('treats empty auth as unauthenticated when no prior auth exists', async () => {
    await expect(resolveAuth(lc, undefined, 'u1', '', undefined)).resolves.toBe(
      undefined,
    );
  });

  test('rejects missing auth when previous auth exists', async () => {
    await expect(
      resolveAuth(lc, {type: 'opaque', raw: 'opaque-1'}, 'u1', '', undefined),
    ).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.Unauthorized,
        origin: ErrorOrigin.ZeroCache,
      },
    });
  });

  test('uses the legacy validator when configured', async () => {
    // oxlint-disable-next-line require-await
    const validateLegacyJWT: ValidateLegacyJWT = async (token, ctx) => ({
      type: 'jwt',
      raw: token,
      decoded: {sub: ctx.userID ?? 'missing-user-id', iat: 1},
    });

    await expect(
      resolveAuth(lc, undefined, 'u1', 'jwt-1', validateLegacyJWT),
    ).resolves.toEqual({
      type: 'jwt',
      raw: 'jwt-1',
      decoded: {sub: 'u1', iat: 1},
    });
  });

  test('rejects authenticated requests without a userID', async () => {
    await expect(
      resolveAuth(lc, undefined, undefined, 'opaque-1', undefined),
    ).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.Unauthorized,
        message: 'Authenticated connections require a userID.',
        origin: ErrorOrigin.ZeroCache,
      },
    });
  });

  test('preserves protocol errors from the validator', async () => {
    // oxlint-disable-next-line require-await
    const validateLegacyJWT: ValidateLegacyJWT = async () => {
      throw new ProtocolError({
        kind: ErrorKind.Unauthorized,
        message: 'nope',
        origin: ErrorOrigin.ZeroCache,
      });
    };

    await expect(
      resolveAuth(lc, undefined, 'u1', 'jwt-1', validateLegacyJWT),
    ).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.Unauthorized,
        message: 'nope',
      },
    });
  });

  test('rejects changing auth type from jwt to opaque', async () => {
    await expect(
      resolveAuth(
        lc,
        {type: 'jwt', raw: 'jwt-1', decoded: {sub: 'u1', iat: 1}},
        'u1',
        'opaque-1',
        undefined,
      ),
    ).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.Unauthorized,
        origin: ErrorOrigin.ZeroCache,
      },
    });
  });

  test('maps validator failures to AuthInvalidated', async () => {
    // oxlint-disable-next-line require-await
    const validateLegacyJWT: ValidateLegacyJWT = async () => {
      throw new Error('bad token');
    };

    await expect(
      resolveAuth(lc, undefined, 'u1', 'jwt-1', validateLegacyJWT),
    ).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.AuthInvalidated,
        origin: ErrorOrigin.ZeroCache,
      },
    });
  });
});

describe('isAuthErrorBody', () => {
  test('matches HTTP auth failures', () => {
    expect(
      isAuthErrorBody({
        kind: ErrorKind.PushFailed,
        origin: ErrorOrigin.ZeroCache,
        reason: ErrorReason.HTTP,
        status: 401,
        bodyPreview: 'Unauthorized',
        message: 'Fetch from API server returned non-OK status 401',
        mutationIDs: [],
      }),
    ).toBe(true);
  });

  test('matches legacy push HTTP auth failures', () => {
    expect(
      isAuthErrorBody({
        error: 'http',
        status: 403,
        details: 'Forbidden',
        mutationIDs: [],
      }),
    ).toBe(true);
  });

  test('does not match non-auth push failures', () => {
    expect(
      isAuthErrorBody({
        error: 'http',
        status: 500,
        details: 'Server error',
        mutationIDs: [],
      }),
    ).toBe(false);
  });
});
