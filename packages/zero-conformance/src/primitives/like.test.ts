import {test} from 'vitest';
import {cases as likeCases} from '../../../zql/src/builder/like-test-cases.ts';
import {getLikePredicate} from '../../../zql/src/builder/like.ts';
import {emitVectors} from '../vectors.ts';

test('like', () => {
  const groups: {pattern: string; flags: 'i' | ''; inputs: string[]}[] = [
    ...likeCases.map(c => ({
      pattern: c.pattern,
      flags: c.flags,
      inputs: c.inputs.map(([input]) => input),
    })),
  ];
  const extraInputs = [
    '',
    'a',
    'A',
    'ab',
    'a\nb',
    '\n',
    '\u{1f600}',
    'a\u{1f600}b',
    'é',
    'É',
    'İ',
    'i',
    'I',
    'ı',
    'ß',
    'SS',
    'ss',
    'Σ',
    'σ',
    'ς',
    '%',
    '_',
    '\\',
    'a.b',
    'a+b',
    '(x)',
    '[x]',
    '^$',
  ];
  const extraPatterns = [
    '',
    '%',
    '_',
    '__',
    'a_b',
    'a%b',
    '\\%',
    '\\_',
    '\\\\',
    'a\\b',
    '%\u{1f600}%',
    '_\u{1f600}',
    '\u{1f600}',
    '__b',
    'a.b',
    'a+b',
    '(x)',
    '[x]',
    '^$',
    'é',
    'e%',
    'İ',
    'i',
    'ß',
    'ss',
    'σ',
    '%Σ%',
    'a\\',
  ];
  for (const pattern of extraPatterns) {
    for (const flags of ['', 'i'] as const) {
      groups.push({pattern, flags, inputs: extraInputs});
    }
  }
  emitVectors(
    'like',
    'like',
    'LIKE / ILIKE (flags "i") predicate results; `throws` when the pattern is invalid.',
    groups.map(({pattern, flags, inputs}) => {
      let predicate;
      try {
        predicate = getLikePredicate(pattern, flags);
      } catch {
        return {pattern, flags, throws: true, results: []};
      }
      return {
        pattern,
        flags,
        throws: false,
        results: inputs.map(input => ({input, match: predicate(input)})),
      };
    }),
  );
});
