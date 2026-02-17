import {assert, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {AuthSessionImpl, pickToken, type ValidateLegacyJWT} from './auth.ts';

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

describe('AuthSession', () => {
  const lc = createSilentLogContext();

  test('binds first userID and rejects mismatched userID', async () => {
    const authSession = new AuthSessionImpl(lc, 'cg1', undefined);
    expect(await authSession.update('u1', 't1')).toEqual({ok: true});

    const mismatch = await authSession.update('u2', 't2');
    assert(!mismatch.ok);
    expect(mismatch.error.kind).toBe(ErrorKind.Unauthorized);
  });

  test('same opaque token does not change revision', async () => {
    const authSession = new AuthSessionImpl(lc, 'cg1', undefined);
    expect(authSession.revision).toBe(0);
    expect(await authSession.update('u1', 't1')).toEqual({ok: true});
    expect(authSession.revision).toBe(1);
    expect(authSession.auth?.raw).toBe('t1');

    expect(await authSession.update('u1', 't1')).toEqual({ok: true});
    expect(authSession.revision).toBe(1);
  });

  test('rejects missing auth if client group is authenticated', async () => {
    const authSession = new AuthSessionImpl(lc, 'cg1', undefined);
    expect(await authSession.update('u1', 't1')).toEqual({ok: true});
    const result = await authSession.update('u1', '');
    assert(!result.ok);
    expect(result.error.kind).toBe(ErrorKind.Unauthorized);
    expect(result.error.message).toContain('No token provided');
  });

  test('treats empty token as missing auth when unauthenticated', async () => {
    const authSession = new AuthSessionImpl(lc, 'cg1', undefined);
    expect(await authSession.update('u1', '')).toEqual({ok: true});
    expect(authSession.auth?.raw).toBe(undefined);
    expect(authSession.revision).toBe(0);
  });

  test('opaque updates revise only on material changes', async () => {
    const authSession = new AuthSessionImpl(lc, 'cg1', undefined);

    expect(await authSession.update('u1', 't1')).toEqual({ok: true});
    expect(authSession.auth?.raw).toBe('t1');
    expect(authSession.revision).toBe(1);

    expect(await authSession.update('u1', 't1')).toEqual({ok: true});
    expect(authSession.revision).toBe(1);

    expect(await authSession.update('u1', 't2')).toEqual({ok: true});
    expect(authSession.auth?.raw).toBe('t2');
    expect(authSession.revision).toBe(2);

    expect(await authSession.update('u1', '')).toEqual({
      ok: false,
      error: {
        kind: ErrorKind.Unauthorized,
        message:
          'No token provided. An unauthenticated client cannot connect to an authenticated client group.',
        origin: ErrorOrigin.ZeroCache,
      },
    });
    expect(authSession.auth?.raw).toBe('t2');
    expect(authSession.revision).toBe(2);

    expect(await authSession.update('u1', 't3')).toEqual({ok: true});
    expect(authSession.auth?.raw).toBe('t3');
    expect(authSession.revision).toBe(3);
  });

  test('legacy validator receives bound userID and updates JWT auth', async () => {
    const validateLegacyJWT: ValidateLegacyJWT = (token, ctx) =>
      Promise.resolve({
        type: 'jwt',
        raw: token,
        decoded: {sub: ctx.userID, iat: 1},
      });
    const authSession = new AuthSessionImpl(lc, 'cg1', validateLegacyJWT);

    expect(await authSession.update('u1', 'jwt-1')).toEqual({ok: true});
    expect(authSession.auth).toEqual({
      type: 'jwt',
      raw: 'jwt-1',
      decoded: {sub: 'u1', iat: 1},
    });
  });

  test('legacy validator failure maps to AuthInvalidated', async () => {
    const validateLegacyJWT: ValidateLegacyJWT = () =>
      Promise.reject(new Error('bad token'));
    const authSession = new AuthSessionImpl(lc, 'cg1', validateLegacyJWT);

    const result = await authSession.update('u1', 'jwt-1');
    assert(!result.ok);
    expect(result.error.kind).toBe(ErrorKind.AuthInvalidated);
    expect(result.error.origin).toBe(ErrorOrigin.ZeroCache);
  });

  test('protocol errors from validator are preserved', async () => {
    const validateLegacyJWT: ValidateLegacyJWT = () =>
      Promise.reject(
        new ProtocolError({
          kind: ErrorKind.Unauthorized,
          message: 'nope',
          origin: ErrorOrigin.ZeroCache,
        }),
      );
    const authSession = new AuthSessionImpl(lc, 'cg1', validateLegacyJWT);

    const result = await authSession.update('u1', 'jwt-1');
    assert(!result.ok);
    expect(result.error.kind).toBe(ErrorKind.Unauthorized);
    expect(result.error.message).toBe('nope');
  });

  test('failed first validation does not bind userID', async () => {
    const validateLegacyJWT: ValidateLegacyJWT = (token, ctx) => {
      if (token === 'bad-token') {
        return Promise.reject(new Error('bad token'));
      }
      return Promise.resolve({
        type: 'jwt',
        raw: token,
        decoded: {sub: ctx.userID, iat: 1},
      });
    };
    const authSession = new AuthSessionImpl(lc, 'cg1', validateLegacyJWT);

    const failed = await authSession.update('alice', 'bad-token');
    assert(!failed.ok);
    expect(failed.error.kind).toBe(ErrorKind.AuthInvalidated);

    expect(await authSession.update('bob', 'good-token')).toEqual({ok: true});
    expect(authSession.auth).toEqual({
      type: 'jwt',
      raw: 'good-token',
      decoded: {sub: 'bob', iat: 1},
    });
  });

  test('clear() resets auth state', async () => {
    const authSession = new AuthSessionImpl(lc, 'cg1', undefined);
    expect(await authSession.update('u1', 't1')).toEqual({ok: true});
    expect(authSession.auth?.raw).toBe('t1');
    expect(authSession.revision).toBe(1);

    authSession.clear();
    expect(authSession.auth?.raw).toBe(undefined);
    expect(authSession.revision).toBe(0);
  });
});
