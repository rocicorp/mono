import {describe, expect, test} from 'vitest';
import * as PostgresTypeClass from '../../../db/postgres-type-class-enum.ts';
import {
  BOOL,
  FLOAT8,
  INT2,
  INT4,
  INT8,
  NUMERIC,
  TEXT,
  TIMESTAMPTZ,
  UUID,
  VARCHAR,
} from '../../../types/pg-types.ts';
import {
  InvalidMarkError,
  isCheaplyOrderable,
  isResumableColumn,
  isResumableKey,
  keyLiteral,
  markOfLastRow,
  orderByRowKey,
  publicationRowFilter,
  resumeWhere,
  textKey,
  type ResumeColumnSpec,
} from './backfill-resume.ts';

const int8: ResumeColumnSpec = {typeOID: INT8};
const int4: ResumeColumnSpec = {typeOID: INT4};
const text: ResumeColumnSpec = {typeOID: TEXT, collationIsDeterministic: true};
const uuid: ResumeColumnSpec = {typeOID: UUID};
const bool: ResumeColumnSpec = {typeOID: BOOL};

describe('isResumableColumn', () => {
  test.each([
    ['int2', {typeOID: INT2}, true],
    ['int4', {typeOID: INT4}, true],
    ['int8', {typeOID: INT8}, true],
    ['uuid', {typeOID: UUID}, true],
    ['bool', {typeOID: BOOL}, true],
    ['text (deterministic)', text, true],
    [
      'varchar (deterministic)',
      {typeOID: VARCHAR, collationIsDeterministic: true},
      true,
    ],
    [
      'text (non-deterministic)',
      {typeOID: TEXT, collationIsDeterministic: false},
      false,
    ],
    ['text (unknown collation)', {typeOID: TEXT}, false],
    ['timestamptz', {typeOID: TIMESTAMPTZ}, false],
    ['numeric', {typeOID: NUMERIC}, false],
    ['float8', {typeOID: FLOAT8}, false],
    [
      'int4 array',
      {typeOID: INT4, elemPgTypeClass: PostgresTypeClass.Base},
      false,
    ],
    ['enum', {typeOID: 90210, pgTypeClass: PostgresTypeClass.Enum}, false],
  ] as [string, ResumeColumnSpec, boolean][])('%s', (_name, spec, expected) => {
    expect(isResumableColumn(spec)).toBe(expected);
  });
});

describe('isResumableKey', () => {
  test('all columns resumable', () => {
    expect(isResumableKey([int8, text, uuid])).toBe(true);
  });

  test('one non-resumable column poisons the key', () => {
    expect(isResumableKey([int8, {typeOID: NUMERIC}])).toBe(false);
  });

  test('an empty key is not resumable', () => {
    expect(isResumableKey([])).toBe(false);
  });
});

describe('keyLiteral', () => {
  test.each([
    ['0', '0'],
    ['123', '123'],
    ['-123', '-123'],
    ['9223372036854775807', '9223372036854775807'],
  ])('int %s', (mark, expected) => {
    expect(keyLiteral(int8, mark)).toBe(expected);
  });

  test.each([
    '1; DROP TABLE foo',
    "1'",
    '1.5',
    '',
    ' 1',
    '1 ',
    '0x10',
    '+1',
    '1e3',
  ])('rejects non-integer %s', mark => {
    expect(() => keyLiteral(int4, mark)).toThrow(InvalidMarkError);
  });

  test('uuid is validated and lowercased', () => {
    expect(keyLiteral(uuid, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11')).toBe(
      `'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid`,
    );
  });

  test.each([
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1',
    'a0eebc999c0b4ef8bb6d6bb9bd380a11',
    `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'; DROP TABLE foo; --`,
    '',
  ])('rejects non-uuid %s', mark => {
    expect(() => keyLiteral(uuid, mark)).toThrow(InvalidMarkError);
  });

  test.each([
    ['true', 'true'],
    ['false', 'false'],
  ])('bool %s', (mark, expected) => {
    expect(keyLiteral(bool, mark)).toBe(expected);
  });

  test.each(['t', 'f', '1', 'TRUE'])('rejects non-bool %s', mark => {
    expect(() => keyLiteral(bool, mark)).toThrow(InvalidMarkError);
  });

  test.each([
    ['plain', `E'plain'`],
    [`it's`, `E'it\\'s'`],
    [`'; DROP TABLE foo; --`, `E'\\'; DROP TABLE foo; --'`],
    ['back\\slash', `E'back\\\\slash'`],
    ['trailing\\', `E'trailing\\\\'`],
    [`\\'`, `E'\\\\\\''`],
    ['new\nline', `E'new\nline'`],
    ['héllo 中文 🎉', `E'héllo 中文 🎉'`],
    ['', `E''`],
  ])('text %j', (mark, expected) => {
    expect(keyLiteral(text, mark)).toBe(expected);
  });

  test('rejects a non-resumable type', () => {
    expect(() => keyLiteral({typeOID: NUMERIC}, '1')).toThrow(InvalidMarkError);
  });
});

describe('orderByRowKey', () => {
  test('quotes identifiers', () => {
    expect(orderByRowKey(['a', 'B c', 'd"e'])).toBe(`"a","B c","d""e"`);
  });
});

describe('resumeWhere', () => {
  test('single column', () => {
    expect(resumeWhere(['id'], [int8], ['42'])).toBe(`("id") > (42)`);
  });

  test('composite key', () => {
    expect(resumeWhere(['id', 'name'], [int8, text], ['42', 'foo'])).toBe(
      `("id","name") > (42,E'foo')`,
    );
  });

  test('mark arity must match the key', () => {
    expect(() => resumeWhere(['id', 'name'], [int8, text], ['42'])).toThrow(
      InvalidMarkError,
    );
  });
});

describe('publicationRowFilter', () => {
  const spec = (publications: Record<string, {rowFilter: string | null}>) =>
    ({publications}) as Parameters<typeof publicationRowFilter>[0];

  test('no filters', () => {
    expect(publicationRowFilter(spec({p: {rowFilter: null}}))).toBe(null);
  });

  test('one filter', () => {
    expect(publicationRowFilter(spec({p: {rowFilter: 'a > 10'}}))).toBe(
      '(a > 10)',
    );
  });

  test('filters are OR-ed and parenthesized', () => {
    expect(
      publicationRowFilter(
        spec({p: {rowFilter: 'a > 10'}, q: {rowFilter: 'b < 5'}}),
      ),
    ).toBe('(a > 10 OR b < 5)');
  });
});

describe('textKey', () => {
  test.each([
    [int8, 123n, '123'],
    [int8, -123n, '-123'],
    [int4, 123, '123'],
    [int8, '123', '123'],
    [text, 'foo', 'foo'],
    [text, '', ''],
    [
      uuid,
      'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    ],
    [bool, 1, 'true'],
    [bool, 0, 'false'],
    [bool, true, 'true'],
    [bool, false, 'false'],
    [bool, 't', 'true'],
    [bool, 'f', 'false'],
  ] as [ResumeColumnSpec, unknown, string][])(
    '%o %o -> %s',
    (spec, value, expected) => {
      expect(textKey(spec, value)).toBe(expected);
    },
  );

  test('round trips through keyLiteral', () => {
    for (const value of ['plain', `it's`, 'back\\slash', 'héllo 中文 🎉']) {
      expect(keyLiteral(text, textKey(text, value))).toBe(
        keyLiteral(text, value),
      );
    }
  });

  test('rejects null', () => {
    expect(() => textKey(int8, null)).toThrow(InvalidMarkError);
  });
});

describe('markOfLastRow', () => {
  test('takes the leading row key values of the last row', () => {
    expect(
      markOfLastRow(
        [int8, text],
        [
          [1n, 'a', 'ignored'],
          [2n, 'b', 'ignored'],
        ],
      ),
    ).toEqual(['2', 'b']);
  });
});

describe('isCheaplyOrderable', () => {
  test.each([
    [1, true],
    [0.99995, true],
    [0.9999, true],
    [-1, true], // reverse heap order is just as seek-free
    [0.9998, false],
    [0.995, false],
    [0.5, false],
    [0, false],
    [null, false], // never analyzed
  ] as [number | null, boolean][])('%s -> %s', (correlation, expected) => {
    expect(isCheaplyOrderable(correlation)).toBe(expected);
  });

  test('the threshold is overridable', () => {
    expect(isCheaplyOrderable(0.9, 0.9)).toBe(true);
    expect(isCheaplyOrderable(0.9, 0.95)).toBe(false);
  });
});
