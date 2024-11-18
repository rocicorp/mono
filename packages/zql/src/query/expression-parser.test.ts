import {expect, test} from 'vitest';
import {parse, stringify} from './expression-test-util.js';

test('parse', () => {
  expect(parse('A & B')).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "left": {
            "name": "n/a",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "A",
          },
          "type": "simple",
        },
        {
          "left": {
            "name": "n/a",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "B",
          },
          "type": "simple",
        },
      ],
      "type": "and",
    }
  `);
  expect(parse('A | B')).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "left": {
            "name": "n/a",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "A",
          },
          "type": "simple",
        },
        {
          "left": {
            "name": "n/a",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "B",
          },
          "type": "simple",
        },
      ],
      "type": "or",
    }
  `);
  expect(parse('A = 2')).toMatchInlineSnapshot(`
    {
      "left": {
        "name": "A",
        "type": "column",
      },
      "op": "=",
      "right": {
        "type": "literal",
        "value": "2",
      },
      "type": "simple",
    }
  `);
  expect(parse('A = 2 | B <= abc')).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "left": {
            "name": "A",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "2",
          },
          "type": "simple",
        },
        {
          "left": {
            "name": "B",
            "type": "column",
          },
          "op": "<=",
          "right": {
            "type": "literal",
            "value": "abc",
          },
          "type": "simple",
        },
      ],
      "type": "or",
    }
  `);
  expect(parse('A = 2 | EXISTS () | C')).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "left": {
            "name": "A",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "2",
          },
          "type": "simple",
        },
        {
          "op": "EXISTS",
          "related": {},
          "type": "correlatedSubquery",
        },
        {
          "left": {
            "name": "n/a",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "C",
          },
          "type": "simple",
        },
      ],
      "type": "or",
    }
  `);
  expect(parse('A = 2 | NOT EXISTS () | C')).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "left": {
            "name": "A",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "2",
          },
          "type": "simple",
        },
        {
          "op": "NOT EXISTS",
          "related": {},
          "type": "correlatedSubquery",
        },
        {
          "left": {
            "name": "n/a",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "C",
          },
          "type": "simple",
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
