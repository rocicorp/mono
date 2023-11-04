import {expect, test} from '@jest/globals';
import {padColumns} from './table.js';

test('right padding', () => {
  expect(
    padColumns([
      ['foo', 'bar', 'baz'],
      ['', '12345', ''],
      ['1234567890', 'boo', ''],
    ]),
  ).toEqual([
    ['foo       ', 'bar  ', 'baz'],
    ['          ', '12345', '   '],
    ['1234567890', 'boo  ', '   '],
  ]);
});

test('left padding', () => {
  expect(
    padColumns(
      [
        ['foo', 'bar', 'baz'],
        ['', '12345', ''],
        ['1234567890', 'boo', ''],
      ],
      '0',
      'left',
    ),
  ).toEqual([
    ['0000000foo', '00bar', 'baz'],
    ['0000000000', '12345', '000'],
    ['1234567890', '00boo', '000'],
  ]);
});
