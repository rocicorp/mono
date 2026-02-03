import {expect, test} from 'vitest';
import {
  majorVersionFromString,
  majorVersionToString,
  stateVersionFromString,
  stateVersionToString,
  type StateVersion,
} from './state-version.ts';

test.each([
  ['00', {major: 0n}],
  ['01', {major: 1n}],
  ['123', {major: 75n}],
  ['01.00', {major: 1n, minor: 0n}],
  ['01.123', {major: 1n, minor: 75n}],
] satisfies [string, StateVersion][])(
  'StateVersion: %s',
  (str, ver: StateVersion) => {
    expect(stateVersionFromString(str)).toEqual(ver);
    expect(stateVersionToString(ver)).toEqual(str);
    expect(majorVersionFromString(str)).toEqual(ver.major);

    if (ver.minor === undefined) {
      expect(majorVersionToString(ver.major)).toBe(str);
    } else {
      expect(str.startsWith(majorVersionToString(ver.major) + '.')).toBe(true);
    }
  },
);

test('sorting', () => {
  const vers = [
    {major: 75n},
    {major: 75n, minor: 1n},
    {major: 23n, minor: 100n},
    {major: 12n, minor: 1001n},
    {major: 23n, minor: 101n},
    {major: 12n, minor: 1n},
    {major: 12n, minor: 0n},
    {major: 12n},
  ];

  expect(
    vers.map(stateVersionToString).sort().map(stateVersionFromString),
  ).toEqual([
    {major: 12n},
    {major: 12n, minor: 0n},
    {major: 12n, minor: 1n},
    {major: 12n, minor: 1001n},
    {major: 23n, minor: 100n},
    {major: 23n, minor: 101n},
    {major: 75n},
    {major: 75n, minor: 1n},
  ]);
});
