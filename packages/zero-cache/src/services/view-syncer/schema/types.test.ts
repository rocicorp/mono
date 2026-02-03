import {describe, expect, test} from 'vitest';
import {
  type CVRVersion,
  cmpVersions,
  cookieToVersion,
  oneAfter,
  versionToNullableCookie,
} from './types.ts';

describe('view-syncer/schema/types', () => {
  test('version comparison', () => {
    expect(
      cmpVersions(
        {stateVersion: '02', configVersion: 1},
        {stateVersion: '01', configVersion: 2},
      ),
    ).toBeGreaterThan(0);

    expect(
      cmpVersions(
        {stateVersion: '01', configVersion: 2},
        {stateVersion: '02', configVersion: 1},
      ),
    ).toBeLessThan(0);

    expect(
      cmpVersions(
        {stateVersion: '02', configVersion: 1},
        {stateVersion: '02', configVersion: 2},
      ),
    ).toBeLessThan(0);

    expect(
      cmpVersions(
        {stateVersion: '02', configVersion: 2},
        {stateVersion: '02', configVersion: 1},
      ),
    ).toBeGreaterThan(0);

    expect(
      cmpVersions({stateVersion: '02'}, {stateVersion: '02', configVersion: 1}),
    ).toBeLessThan(0);

    expect(
      cmpVersions({stateVersion: '02', configVersion: 1}, {stateVersion: '02'}),
    ).toBeGreaterThan(0);

    expect(
      cmpVersions(
        {stateVersion: '02', configVersion: 2},
        {stateVersion: '02', configVersion: 2},
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
      {cookie: '00:01', version: {stateVersion: '00', configVersion: 1}},
      {cookie: '100:0a', version: {stateVersion: '100', configVersion: 10}},
      {
        cookie: 'a128adk2f9s:110',
        version: {stateVersion: 'a128adk2f9s', configVersion: 36},
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
        {reason: 'too many colons', cookie: '1:2:3'},
        {reason: 'minor version too big', cookie: '110:93jlxpt2ps'},
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

  (
    [
      {
        version: {stateVersion: '00'},
        plusOne: {stateVersion: '00', configVersion: 1},
      },
      {
        version: {stateVersion: '2abc'},
        plusOne: {stateVersion: '2abc', configVersion: 1},
      },
      {
        version: {stateVersion: '00', configVersion: 1},
        plusOne: {stateVersion: '00', configVersion: 2},
      },
      {
        version: {stateVersion: '100', configVersion: 10},
        plusOne: {stateVersion: '100', configVersion: 11},
      },
      {
        version: {stateVersion: 'a128adk2f9s', configVersion: 36},
        plusOne: {stateVersion: 'a128adk2f9s', configVersion: 37},
      },
    ] satisfies {
      version: CVRVersion;
      plusOne: CVRVersion;
    }[]
  ).forEach(c => {
    test(`oneAfter version ${JSON.stringify(c.version)}`, () => {
      expect(oneAfter(c.version)).toEqual(c.plusOne);
    });
  });
});
