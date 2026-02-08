import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {ProtocolError} from '../../../../zero-protocol/src/error.ts';
import {pickToken} from './view-syncer.ts';

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

  test('previous token has a subject, new token does not', () => {
    expect(() =>
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo'}, raw: ''},
        {type: 'jwt', decoded: {}, raw: ''},
      ),
    ).toThrowError(ProtocolError);
  });

  test('previous token has a subject, new token has a different subject', () => {
    expect(() =>
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo', iat: 1}, raw: ''},
        {type: 'jwt', decoded: {sub: 'bar', iat: 1}, raw: ''},
      ),
    ).toThrowError(ProtocolError);
  });

  test('previous token has a subject, new token has the same subject', () => {
    expect(
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo', iat: 1}, raw: ''},
        {type: 'jwt', decoded: {sub: 'foo', iat: 2}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
      type: 'jwt',
    });

    expect(
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo', iat: 2}, raw: ''},
        {type: 'jwt', decoded: {sub: 'foo', iat: 1}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
      type: 'jwt',
    });
  });

  test('previous token has no subject, new token has a subject', () => {
    expect(() =>
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo', iat: 123}, raw: ''},
        {type: 'jwt', decoded: {iat: 123}, raw: ''},
      ),
    ).toThrowError(ProtocolError);
  });

  test('previous token has no subject, new token has no subject', () => {
    expect(
      pickToken(
        lc,
        {type: 'jwt', decoded: {iat: 1}, raw: ''},
        {type: 'jwt', decoded: {iat: 2}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        iat: 2,
      },
      raw: '',
      type: 'jwt',
    });
    expect(
      pickToken(
        lc,
        {type: 'jwt', decoded: {iat: 2}, raw: ''},
        {type: 'jwt', decoded: {iat: 1}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        iat: 2,
      },
      raw: '',
      type: 'jwt',
    });
  });

  test('previous token has an issued at time, new token does not', () => {
    expect(() =>
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo', iat: 1}, raw: ''},
        {type: 'jwt', decoded: {sub: 'foo'}, raw: ''},
      ),
    ).toThrowError(ProtocolError);
  });

  test('previous token has an issued at time, new token has a greater issued at time', () => {
    expect(
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo', iat: 1}, raw: ''},
        {type: 'jwt', decoded: {sub: 'foo', iat: 2}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
      type: 'jwt',
    });
  });

  test('previous token has an issued at time, new token has a lesser issued at time', () => {
    expect(
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo', iat: 2}, raw: ''},
        {type: 'jwt', decoded: {sub: 'foo', iat: 1}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
      type: 'jwt',
    });
  });

  test('previous token has an issued at time, new token has the same issued at time', () => {
    expect(
      pickToken(
        lc,
        {
          type: 'jwt',
          decoded: {sub: 'foo', iat: 2},
          raw: '',
        },
        {
          type: 'jwt',
          decoded: {sub: 'foo', iat: 2},
          raw: '',
        },
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
      type: 'jwt',
    });
  });

  test('previous token has no issued at time, new token has an issued at time', () => {
    expect(
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo'}, raw: 'no-iat'},
        {type: 'jwt', decoded: {sub: 'foo', iat: 2}, raw: 'iat'},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: 'iat',
      type: 'jwt',
    });
  });

  test('previous token has no issued at time, new token has no issued at time', () => {
    expect(
      pickToken(
        lc,
        {type: 'jwt', decoded: {sub: 'foo'}, raw: ''},
        {type: 'jwt', decoded: {sub: 'foo'}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
      },
      raw: '',
      type: 'jwt',
    });
  });
});
