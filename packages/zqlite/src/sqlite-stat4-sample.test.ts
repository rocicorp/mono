import {expect, test} from 'vitest';
import {
  decodeSampleKinds,
  isSampleNull,
  type SampleValueKind,
} from './sqlite-stat4-sample.ts';

/**
 * Builds a record with the given serial types, as SQLite writes one: a header
 * size varint, then one varint per serial type. The data that follows the
 * header is irrelevant here, so it is left off.
 */
function record(...serialTypes: number[]): Buffer {
  const types = Buffer.from(serialTypes);
  return Buffer.concat([Buffer.from([types.length + 1]), types]);
}

test.each<[string, Buffer, SampleValueKind[]]>([
  ['null', record(0), ['null']],
  ['1-byte int', record(1), ['integer']],
  ['6-byte int', record(6), ['integer']],
  ['real', record(7), ['real']],
  ['int 0 and int 1', record(8, 9), ['integer', 'integer']],
  ['internal types', record(10, 11), ['unknown', 'unknown']],
  ['blob (even >= 12)', record(12), ['blob']],
  ['text (odd >= 13)', record(13), ['text']],
  ['longer blob and text', record(30, 41), ['blob', 'text']],
  ['index key plus rowid', record(9, 21, 1), ['integer', 'text', 'integer']],
])('%s', (_name, sample, expected) => {
  expect(decodeSampleKinds(sample)).toEqual(expected);
});

test('multi-byte varints', () => {
  // A header of 130 bytes: varint 0x81 0x02, then serial types, one of which
  // is a long text (varint 0x81 0x03 = 131, odd, so text of 59 bytes).
  const sample = Buffer.concat([
    Buffer.from([0x81, 0x02]),
    Buffer.from([0x09]),
    Buffer.from([0x81, 0x03]),
    Buffer.alloc(130 - 5, 1), // filler serial types, each a 1-byte int
    Buffer.alloc(200), // data
  ]);
  const kinds = decodeSampleKinds(sample);
  expect(kinds.slice(0, 2)).toEqual(['integer', 'text']);
  expect(kinds).toHaveLength(2 + (130 - 5));
});

test.each([
  ['empty', Buffer.of()],
  ['header longer than the record', Buffer.of(40, 1, 1)],
  ['header size of 0', Buffer.of(0, 1)],
  ['truncated varint', Buffer.of(4, 0x81, 0x81)],
])('malformed: %s', (_name, sample) => {
  expect(decodeSampleKinds(sample)).toEqual([]);
  // A sample we cannot decode is treated as NULL, which is the conservative
  // choice for fanout: such samples are excluded from the non-NULL average.
  expect(isSampleNull(sample)).toBe(true);
});

test('isSampleNull', () => {
  expect(isSampleNull(record(0, 21))).toBe(true);
  expect(isSampleNull(record(21, 0))).toBe(false);
});
