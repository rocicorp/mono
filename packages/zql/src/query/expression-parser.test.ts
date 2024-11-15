import {expect, test} from 'vitest';
import {parse, stringify} from './expression-parser.js';

test('parse', () => {
  expect(parse('A & B')).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "field": "n/a",
          "op": "=",
          "type": "simple",
          "value": "A",
        },
        {
          "field": "n/a",
          "op": "=",
          "type": "simple",
          "value": "B",
        },
      ],
      "type": "and",
    }
  `);
  expect(parse('A | B')).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "field": "n/a",
          "op": "=",
          "type": "simple",
          "value": "A",
        },
        {
          "field": "n/a",
          "op": "=",
          "type": "simple",
          "value": "B",
        },
      ],
      "type": "or",
    }
  `);
  expect(parse('A = 2')).toMatchInlineSnapshot(`
    {
      "field": "A",
      "op": "=",
      "type": "simple",
      "value": "2",
    }
  `);
  expect(parse('A = 2 | B <= abc')).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "field": "A",
          "op": "=",
          "type": "simple",
          "value": "2",
        },
        {
          "field": "B",
          "op": "<=",
          "type": "simple",
          "value": "abc",
        },
      ],
      "type": "or",
    }
  `);
  expect(parse('A = 2 | EXISTS () | C')).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "field": "A",
          "op": "=",
          "type": "simple",
          "value": "2",
        },
        {
          "op": "EXISTS",
          "related": {},
          "type": "correlatedSubquery",
        },
        {
          "field": "n/a",
          "op": "=",
          "type": "simple",
          "value": "C",
        },
      ],
      "type": "or",
    }
  `);
  expect(parse('A = 2 | NOT EXISTS () | C')).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "field": "A",
          "op": "=",
          "type": "simple",
          "value": "2",
        },
        {
          "op": "NOT EXISTS",
          "related": {},
          "type": "correlatedSubquery",
        },
        {
          "field": "n/a",
          "op": "=",
          "type": "simple",
          "value": "C",
        },
      ],
      "type": "or",
    }
  `);
});

test.for([
  'x = 1',
  'A != 1',
  'abc < 1',
  'def123 > 1',
  'A <= 1',
  'A >= 1',
  'A & B',
  'A | B',
  'A = 2',
  'A = 2 | B <= abc',
  'A = 2 | EXISTS () | C',
  '(x < 2 & y > 3) | NOT EXISTS ()',
  'x IN abc | y NOT IN def',
  'x LIKE abc | y NOT LIKE def',
  'x ILIKE abc | y NOT ILIKE def',
  'NOT EXISTS () | NOT EXISTS ()',
  'NOT EXISTS () | EXISTS ()',
])('roundtrip %s', (input: string) => {
  expect(stringify(parse(input))).toEqual(input);
});
