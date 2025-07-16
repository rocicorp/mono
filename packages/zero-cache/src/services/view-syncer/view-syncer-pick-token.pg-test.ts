import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {ErrorForClient} from '../../types/error-for-client.ts';
import {pickToken} from './view-syncer.ts';

describe('pickToken', () => {
  const lc = createSilentLogContext();

  test('previous token is undefined', () => {
    expect(
      pickToken(lc, undefined, {decoded: {sub: 'foo', iat: 1}, raw: ''}),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 1,
      },
      raw: '',
    });
  });

  test('previous token exists, new token is undefined', () => {
    expect(() =>
      pickToken(lc, {decoded: {sub: 'foo', iat: 1}, raw: ''}, undefined),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has a subject, new token does not', () => {
    expect(() =>
      pickToken(lc, {decoded: {sub: 'foo'}, raw: ''}, {decoded: {}, raw: ''}),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has a subject, new token has a different subject', () => {
    expect(() =>
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
        {decoded: {sub: 'bar', iat: 1}, raw: ''},
      ),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has a subject, new token has the same subject', () => {
    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
        {decoded: {sub: 'foo', iat: 2}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
    });

    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 2}, raw: ''},
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
    });
  });

  test('previous token has no subject, new token has a subject', () => {
    expect(() =>
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 123}, raw: ''},
        {decoded: {iat: 123}, raw: ''},
      ),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has no subject, new token has no subject', () => {
    expect(
      pickToken(lc, {decoded: {iat: 1}, raw: ''}, {decoded: {iat: 2}, raw: ''}),
    ).toEqual({
      decoded: {
        iat: 2,
      },
      raw: '',
    });
    expect(
      pickToken(lc, {decoded: {iat: 2}, raw: ''}, {decoded: {iat: 1}, raw: ''}),
    ).toEqual({
      decoded: {
        iat: 2,
      },
      raw: '',
    });
  });

  test('previous token has an issued at time, new token does not', () => {
    expect(() =>
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
        {decoded: {sub: 'foo'}, raw: ''},
      ),
    ).toThrowError(ErrorForClient);
  });

  test('previous token has an issued at time, new token has a greater issued at time', () => {
    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
        {decoded: {sub: 'foo', iat: 2}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
    });
  });

  test('previous token has an issued at time, new token has a lesser issued at time', () => {
    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo', iat: 2}, raw: ''},
        {decoded: {sub: 'foo', iat: 1}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: '',
    });
  });

  test('previous token has an issued at time, new token has the same issued at time', () => {
    expect(
      pickToken(
        lc,
        {
          decoded: {sub: 'foo', iat: 2},
          raw: '',
        },
        {
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
    });
  });

  test('previous token has no issued at time, new token has an issued at time', () => {
    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo'}, raw: 'no-iat'},
        {decoded: {sub: 'foo', iat: 2}, raw: 'iat'},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
        iat: 2,
      },
      raw: 'iat',
    });
  });

  test('previous token has no issued at time, new token has no issued at time', () => {
    expect(
      pickToken(
        lc,
        {decoded: {sub: 'foo'}, raw: ''},
        {decoded: {sub: 'foo'}, raw: ''},
      ),
    ).toEqual({
      decoded: {
        sub: 'foo',
      },
      raw: '',
    });
  });
});
