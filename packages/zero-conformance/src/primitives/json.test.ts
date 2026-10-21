import {test} from 'vitest';
import {float64Bits, interestingStrings, Random} from '../random.ts';
import {emitVectors} from '../vectors.ts';

const r = new Random(0x15011);

test('number-format', () => {
  const numbers: number[] = [
    0,
    -0,
    1,
    -1,
    0.1,
    0.5,
    1.5,
    -1.5,
    100,
    123456789,
    2 ** 31 - 1,
    2 ** 31,
    -(2 ** 31),
    2 ** 32,
    2 ** 53 - 1,
    2 ** 53,
    2 ** 53 + 2,
    1e20,
    1e21,
    1.5e21,
    123456789012345680000,
    1e-6,
    1e-7,
    1.5e-7,
    0.000001234,
    0.0000001234,
    5e-324,
    Number.MAX_VALUE,
    Number.MIN_VALUE,
    Number.EPSILON,
    Math.PI,
    Math.E,
    1 / 3,
    2 / 3,
    0.1 + 0.2,
    1e300,
    1e-300,
    NaN,
    Infinity,
    -Infinity,
    4.35,
    0.000123,
    1234.5678,
  ];
  for (let i = 0; i < 300; i++) {
    numbers.push(r.float64());
  }
  for (let i = 0; i < 100; i++) {
    numbers.push(r.int(1e9) / 10 ** r.int(12));
  }
  emitVectors(
    'number-format',
    'number-format',
    '`String(n)` for float64 values given as big-endian hex bit patterns.',
    numbers.map(n => ({bits: float64Bits(n), string: String(n)})),
  );
});

test('number-parse', () => {
  const inputs = [
    '',
    ' ',
    '0',
    '-0',
    '+0',
    '1',
    '-1',
    '+1',
    '1.5',
    '.5',
    '5.',
    '1e3',
    '1E3',
    '1e+3',
    '1e-3',
    '1e',
    'e3',
    ' 42 ',
    '\t42\n',
    ' 42 ',
    '0x10',
    '0X1f',
    '0o17',
    '0b101',
    '-0x10',
    '0x',
    '0xg',
    'Infinity',
    '-Infinity',
    '+Infinity',
    'infinity',
    'NaN',
    'abc',
    '1a',
    '1_000',
    '1,000',
    '00012',
    '1e400',
    '-1e400',
    '1e-400',
    '123456789012345678901234567890',
    '9007199254740993',
    '0.1',
    '10',
    '30',
  ];
  emitVectors(
    'number-parse',
    'number-parse',
    '`Number(s)` as a big-endian hex float64 bit pattern.',
    inputs.map(s => ({input: s, bits: float64Bits(Number(s))})),
  );
});

test('json-parse', () => {
  const ok = [
    'null',
    'true',
    'false',
    '0',
    '-0',
    '1',
    '-1.5e10',
    '1E400',
    '123456789012345678901234567890',
    '0.1',
    '"abc"',
    '""',
    '"\\u00e9\\n\\t\\"\\\\\\/"',
    '"\\ud83d\\ude00"',
    '"😀"',
    '"\\u0000"',
    '[]',
    '[1,2,3]',
    '{}',
    '{"a":1,"b":[true,null,{"c":"d"}]}',
    ' \t\n\r{ "a" : 1 } ',
    '{"a":1,"a":2}',
    '{"__proto__":1}',
    '[[[[[]]]]]',
    '{"":0}',
    '"\\u2028"',
  ];
  const bad = [
    '',
    ' ',
    'nul',
    'True',
    '01',
    '-',
    '+1',
    '.5',
    '1.',
    '1e',
    '0x10',
    'NaN',
    'Infinity',
    '"abc',
    '"\u0001"',
    '"\\x41"',
    '"\\u12"',
    "'a'",
    '[1,]',
    '[1 2]',
    '{"a":1,}',
    '{a:1}',
    '{"a" 1}',
    '[1]x',
    '{"a":1}}',
  ];
  emitVectors(
    'json-parse',
    'json-parse',
    'JSON.parse acceptance; accepted inputs carry their JSON.stringify re-encoding.',
    [
      ...ok.map(input => ({
        input,
        ok: true,
        stringify: JSON.stringify(JSON.parse(input)),
      })),
      ...bad.map(input => {
        try {
          JSON.parse(input);
          throw new Error(`expected ${JSON.stringify(input)} to be rejected`);
        } catch (e) {
          if (!(e instanceof SyntaxError)) {
            throw e;
          }
        }
        return {input, ok: false};
      }),
    ],
  );
});

test('json-stringify-strings', () => {
  const strings = [...interestingStrings];
  for (let i = 0; i < 0x20; i++) {
    strings.push(String.fromCharCode(i));
  }
  strings.push('a"b\\c\u0007d\u007fe\u0080f');
  emitVectors(
    'json-stringify-strings',
    'json-stringify-strings',
    'JSON.stringify of strings (escaping rules).',
    strings.map(s => ({input: s, stringify: JSON.stringify(s)})),
  );
});
