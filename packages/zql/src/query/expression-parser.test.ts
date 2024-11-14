import {expect, test} from 'vitest';
import {parse, stringify} from './expression-parser.js';

test('parse and stringify', () => {
  expect(stringify(parse('A & B'))).toEqual('A & B');
  expect(stringify(parse('A | B'))).toEqual('A | B');
  expect(stringify(parse('A = 2'))).toEqual('A = 2');
  expect(stringify(parse('A = 2 | B <= abc'))).toEqual('A = 2 | B <= abc');
  expect(stringify(parse('A = 2 | EXISTS () | C'))).toEqual(
    'A = 2 | EXISTS () | C',
  );
  expect(stringify(parse('A = 2 | NOT EXISTS () | C'))).toEqual(
    'A = 2 | NOT EXISTS () | C',
  );
});
