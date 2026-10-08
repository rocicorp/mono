import {expect, test} from 'vitest';
import {packBooleanMap, unpackBooleanMap} from './packed-boolean-map.ts';

test.each<{entries: [number, boolean][]; bytes: number[]}>([
  {entries: [], bytes: []},
  {entries: [[0, true]], bytes: [0b11]},
  {entries: [[0, false]], bytes: [0b01]},
  {entries: [[1, true]], bytes: [0b1100]},
  {entries: [[3, false]], bytes: [0b0100_0000]},
  {entries: [[4, true]], bytes: [0, 0b11]},
  {
    entries: [
      [0, false],
      [1, true],
      [5, false],
    ],
    bytes: [0b1101, 0b0100],
  },
])('$entries', ({entries, bytes}) => {
  const map = new Map(entries);
  expect(packBooleanMap(map)).toEqual(new Uint8Array(bytes));
  expect(
    unpackBooleanMap(new Uint8Array(bytes), [0, 1, 2, 3, 4, 5, 6, 7]),
  ).toEqual(map);
});

test('reads only the given keys', () => {
  expect(unpackBooleanMap(new Uint8Array([0b1111, 0b11]), [1, 9])).toEqual(
    new Map([[1, true]]),
  );
});

test('ignores a value bit without its present bit', () => {
  expect(unpackBooleanMap(new Uint8Array([0b10]), [0])).toEqual(new Map());
});

test.each([-1, 1.5, NaN])('rejects key %d', key => {
  expect(() => packBooleanMap(new Map([[key, true]]))).toThrow(
    'Key must be a non-negative integer',
  );
});
