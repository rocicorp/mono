import {describe, expect, test} from 'vitest';
import {compareCookies} from '../../../../../replicache/src/cookies.ts';
import {stateVersionToString} from '../../../types/state-version.ts';
import {
  type CVRVersion,
  cmpVersions,
  cookieToVersion,
  oneAfter,
  versionToNullableCookie,
} from './types.ts';

describe('view-syncer/schema/types', () => {
  // KNOWN DEFECT, pinned as a failing test until it is fixed.
  //
  // A cookie is `stateVersion[:configVersion]`, and a stateVersion is
  // `major` or `major.minor` (the minor is minted by a backfill). The server
  // orders versions structurally (`cmpVersions`: stateVersion, then
  // configVersion). Replicache orders the same cookies as opaque strings
  // (`compareCookies` → `stringCompare`), which is the contract the server
  // has to honor when it mints them — and `.` (0x2E) sorts below `:` (0x3A),
  // so for the same major `major.minor` reads OLDER than `major:config`
  // even though the server says it is newer. `handlePullResponseV1` then
  // refuses the poke that follows a backfill for every client sitting at a
  // `major:config` cookie on that major, clears its poke buffer and
  // disconnects; it reconnects and repeats until an ordinary write advances
  // the major.
  //
  // The inputs are proven by the test before the `test.fails` one, and the
  // failing block holds a single assertion: `test.fails` is satisfied by any
  // throw, so nothing else in it may be able to.
  const heldCookie = () =>
    versionToNullableCookie({
      stateVersion: stateVersionToString({major: 4242n}),
      configVersion: 7,
    });
  const afterBackfillCookie = () =>
    versionToNullableCookie({
      stateVersion: stateVersionToString({major: 4242n, minor: 1n}),
    });

  // The client's answer is computed in the normal test, so a compare that
  // THROWS on this pair fails the file rather than satisfying `test.fails`.
  let clientOrder: number | undefined;

  test('the server orders a post-backfill version above the held one', () => {
    const held = heldCookie();
    const afterBackfill = afterBackfillCookie();
    expect(held).not.toBeNull();
    expect(afterBackfill).not.toBeNull();
    expect(
      cmpVersions(cookieToVersion(afterBackfill), cookieToVersion(held)),
    ).toBeGreaterThan(0);
    clientOrder = compareCookies(afterBackfill, held);
    expect(typeof clientOrder).toBe('number');
  });

  test.fails('the client orders the same cookies the same way', () => {
    // It does not: `.` < `:`.
    expect(clientOrder).toBeGreaterThan(0);
  });

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
