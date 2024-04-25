import {describe, expect, test} from 'vitest';
import {
  CVRVersion,
  cmpVersions,
  cookieToVersion,
  versionToNullableCookie,
} from './types.js';

describe('view-syncer/schema/types', () => {
  test('version comparison', () => {
    expect(
      cmpVersions(
        {stateVersion: '02', minorVersion: 1},
        {stateVersion: '01', minorVersion: 2},
      ),
    ).toBeGreaterThan(0);

    expect(
      cmpVersions(
        {stateVersion: '01', minorVersion: 2},
        {stateVersion: '02', minorVersion: 1},
      ),
    ).toBeLessThan(0);

    expect(
      cmpVersions(
        {stateVersion: '02', minorVersion: 1},
        {stateVersion: '02', minorVersion: 2},
      ),
    ).toBeLessThan(0);

    expect(
      cmpVersions(
        {stateVersion: '02', minorVersion: 2},
        {stateVersion: '02', minorVersion: 1},
      ),
    ).toBeGreaterThan(0);

    expect(
      cmpVersions({stateVersion: '02'}, {stateVersion: '02', minorVersion: 1}),
    ).toBeLessThan(0);

    expect(
      cmpVersions({stateVersion: '02', minorVersion: 1}, {stateVersion: '02'}),
    ).toBeGreaterThan(0);

    expect(
      cmpVersions(
        {stateVersion: '02', minorVersion: 2},
        {stateVersion: '02', minorVersion: 2},
      ),
    ).toBe(0);

    expect(cmpVersions(null, null)).toBe(0);
    expect(cmpVersions(null, {stateVersion: '00'})).toBeLessThan(0);
    expect(cmpVersions({stateVersion: '00'}, null)).toBeGreaterThan(0);
  });

  (
    [
      {cookie: null, version: null},
      {cookie: '00', version: {stateVersion: '00'}},
      {cookie: '2abc', version: {stateVersion: '2abc'}},
      {cookie: '00.01', version: {stateVersion: '00', minorVersion: 1}},
      {cookie: '100.0a', version: {stateVersion: '100', minorVersion: 10}},
      {
        cookie: 'a128adk2f9s.110',
        version: {stateVersion: 'a128adk2f9s', minorVersion: 36},
      },
    ] satisfies {
      cookie: string | null;
      version: CVRVersion | null;
    }[]
  ).forEach(c => {
    test(`cookie <-> version ${c.cookie}`, () => {
      expect(cookieToVersion(c.cookie)).toEqual(c.version);
      expect(versionToNullableCookie(c.version)).toEqual(c.cookie);
    });

    (
      [
        {reason: 'not a lexiversion', cookie: 'foo-bar'},
        {reason: 'too many dots', cookie: '1.2.3'},
        {reason: 'minor version too big', cookie: '110.93jlxpt2ps'},
      ] satisfies {
        reason: string;
        cookie: string;
      }[]
    ).forEach(c => {
      test(`invalid cookie: ${c.reason}`, () => {
        expect(() => cookieToVersion(c.cookie)).toThrowError();
      });
    });
  });
});
