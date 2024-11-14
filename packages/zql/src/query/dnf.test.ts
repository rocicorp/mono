import {expect, test} from 'vitest';
import {
  type Condition,
  type Conjunction,
  type Disjunction,
} from '../../../zero-protocol/src/ast.js';
import {dnf, unwrap} from './dnf.js';
import {parse, stringify} from './expression-parser.js';

function simple(value: number | string): Condition {
  return {type: 'simple', value, op: '=', field: 'n/a'};
}

const A = simple('A');
const B = simple('B');
const C = simple('C');
const D = simple('D');
const E = simple('E');

const and = (...conditions: Condition[]): Conjunction => ({
  type: 'and',
  conditions,
});
const or = (...conditions: Condition[]): Disjunction => ({
  type: 'or',
  conditions,
});

test('unwrap', () => {
  expect(unwrap(A)).toEqual(A);
  expect(unwrap(and(A, B))).toEqual(and(A, B));
  expect(unwrap(or(A, B))).toEqual(or(A, B));
  expect(unwrap(and(or(A)))).toEqual(A);
  expect(unwrap(or(and(or(A))))).toEqual(A);
  expect(unwrap(and(A, or(B), or(or(C))))).toEqual(and(A, B, C));

  // A & (B & C) & (D | E) -> A & B & C & (D | E)
  expect(unwrap(and(A, and(B, C), or(D, E)))).toEqual(and(A, B, C, or(D, E)));

  // A | (B & C) | (D | E) -> A | B & C | D | E
  expect(unwrap(or(A, and(B, C), or(D, E)))).toEqual(or(A, and(B, C), D, E));
});

test.for([
  ['A & B', 'A & B'],
  ['A | B', 'A | B'],
  ['(A | B) & C', '(A & C) | (B & C)'],
  ['(A | B) & (C | D)', '(A & C) | (A & D) | (B & C) | (B & D)'],
  ['(A & B) | C', '(A & B) | C'],
  ['(A & B) |(C & A)', '(A & B) | (C & A)'],
  ['A & (B | (C & D))', '(A & B) | (A & C & D)'],
  [
    'A & (B | (C & (D | (E | F))))',
    '(A & B) | (A & C & D) | (A & C & E) | (A & C & F)',
  ],

  ['EXISTS ()', 'EXISTS ()'],
  ['NOT EXISTS ()', 'NOT EXISTS ()'],
  ['A = 2 | EXISTS () ', 'A = 2 | EXISTS ()'],
])(`dnf: %s -> %s`, ([input, expected]) => {
  expect(stringify(dnf(parse(input)))).toEqual(expected);
  // dnf on a dnf should produce the same thing
  expect(stringify(dnf(dnf(parse(input))))).toEqual(expected);
});
