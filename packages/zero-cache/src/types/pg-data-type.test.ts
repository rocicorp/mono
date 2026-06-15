import {describe, expect, test} from 'vitest';
import {
  dataTypeToZqlValueType,
  isPgNativeStringType,
  isPgNumberType,
  isPgStringType,
  isPgTextRepresentedType,
  pgToZqlNativeStringTypeMap,
  pgToZqlNumericTypeMap,
  pgToZqlStringTypeMap,
  pgToZqlTextRepresentedTypeMap,
} from './pg-data-type.ts';

describe('pg-data-type helpers', () => {
  test.each(Object.keys(pgToZqlNumericTypeMap))(
    'identifies numeric type %s',
    pgType => {
      expect(isPgNumberType(pgType)).toBe(true);
      expect(isPgStringType(pgType)).toBe(false);
    },
  );

  test.each(Object.keys(pgToZqlStringTypeMap))(
    'identifies string type %s',
    pgType => {
      expect(isPgStringType(pgType)).toBe(true);
      expect(isPgNumberType(pgType)).toBe(false);
    },
  );

  test.each(Object.keys(pgToZqlNativeStringTypeMap))(
    'identifies native string type %s',
    pgType => {
      expect(isPgNativeStringType(pgType)).toBe(true);
      expect(isPgTextRepresentedType(pgType)).toBe(false);
    },
  );

  test.each(Object.keys(pgToZqlTextRepresentedTypeMap))(
    'identifies text-represented type %s',
    pgType => {
      expect(isPgTextRepresentedType(pgType)).toBe(true);
      expect(isPgNativeStringType(pgType)).toBe(false);
    },
  );

  test('isPgStringType and isPgNumberType are case insensitive', () => {
    expect(isPgStringType('TEXT')).toBe(true);
    expect(isPgNativeStringType('TEXT')).toBe(true);
    expect(isPgTextRepresentedType('ISBN13')).toBe(true);
    expect(isPgNumberType('INTEGER')).toBe(true);
  });
});

describe('dataTypeToZqlValueType', () => {
  test.each([
    ['smallint', 'number'],
    ['integer', 'number'],
    ['int', 'number'],
    ['int2', 'number'],
    ['int4', 'number'],
    ['int8', 'number'],
    ['bigint', 'number'],
    ['smallserial', 'number'],
    ['serial', 'number'],
    ['serial2', 'number'],
    ['serial4', 'number'],
    ['serial8', 'number'],
    ['bigserial', 'number'],
    ['decimal', 'number'],
    ['numeric', 'number'],
    ['real', 'number'],
    ['double precision', 'number'],
    ['float', 'number'],
    ['float4', 'number'],
    ['float8', 'number'],
    ['date', 'number'],
    ['time', 'number'],
    ['timetz', 'number'],
    ['timestamp', 'number'],
    ['timestamptz', 'number'],
    ['timestamp with time zone', 'number'],
    ['timestamp without time zone', 'number'],
    ['bpchar', 'string'],
    ['character', 'string'],
    ['character varying', 'string'],
    ['cidr', 'string'],
    ['ean13', 'string'],
    ['inet', 'string'],
    ['isbn', 'string'],
    ['isbn13', 'string'],
    ['ismn', 'string'],
    ['ismn13', 'string'],
    ['issn', 'string'],
    ['issn13', 'string'],
    ['macaddr', 'string'],
    ['macaddr8', 'string'],
    ['pg_lsn', 'string'],
    ['text', 'string'],
    ['upc', 'string'],
    ['uuid', 'string'],
    ['varchar', 'string'],
    ['bool', 'boolean'],
    ['boolean', 'boolean'],
    ['json', 'json'],
    ['jsonb', 'json'],
  ])('maps %s to %s', (pgType, expectedType) => {
    expect(dataTypeToZqlValueType(pgType, false, false)).toBe(expectedType);
    // Case insensitive test
    expect(dataTypeToZqlValueType(pgType.toUpperCase(), false, false)).toBe(
      expectedType,
    );
  });

  test.each([['custom_enum_type'], ['another_enum']])(
    'handles enum type %s as string',
    enumType => {
      expect(dataTypeToZqlValueType(enumType, true, false)).toBe('string');
    },
  );

  test.each([['custom_enum_type'], ['another_enum']])(
    'handles enum array type %s as json',
    enumType => {
      expect(dataTypeToZqlValueType(enumType, true, true)).toBe('json');
    },
  );

  test.each([['bytea'], ['unknown_type']])(
    'returns undefined for unmapped type %s',
    unmappedType => {
      expect(
        dataTypeToZqlValueType(unmappedType, false, false),
      ).toBeUndefined();
    },
  );
});
