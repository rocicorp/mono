import {expect, test} from 'vitest';
import {csvBatches, csvField} from './replica-seed.ts';

test('csvField converts replica storage by client type', () => {
  expect(csvField(null, 'string')).toBe('');
  expect(csvField('', 'string')).toBe('""');
  expect(csvField('say "hi", ok', 'string')).toBe('"say ""hi"", ok"');
  expect(csvField(1, 'boolean')).toBe('t');
  expect(csvField(0, 'boolean')).toBe('f');
  expect(csvField(1733699597510.7358, 'number')).toBe('1733699597510.7358');
  expect(csvField('{"a":[1,"x"]}', 'json')).toBe('"{""a"":[1,""x""]}"');
});

test('csvBatches emits one line per row and counts rows', () => {
  let rows = 0;
  const text = [
    ...csvBatches(
      [
        ['a', 1, null],
        ['b', 0, 2.5],
      ],
      ['string', 'boolean', 'number'],
      n => {
        rows += n;
      },
    ),
  ].join('');
  expect(text).toBe('"a",t,\n"b",f,2.5\n');
  expect(rows).toBe(2);
});
