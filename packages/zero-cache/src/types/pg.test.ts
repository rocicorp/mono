/* eslint-disable no-console */
import {describe, expect, test} from 'vitest';
import {dataTypeToZqlValueType, timestampToFpMillis} from './pg.ts';

describe('timestampToFpMillis', () => {
  test.each([
    ['2019-01-11 22:30:35.381101-01', 1547249435381.101],
    ['2019-01-11 23:30:35.381101+00', 1547249435381.101],
    ['2019-01-12 00:30:35.381101+01', 1547249435381.101],

    ['2019-01-11 23:30:35.381101+01:01', 1547245775381.101],
    ['2019-01-11 22:30:35.381101+00:03', 1547245655381.101],

    ['2004-10-19 10:23:54.654321', 1098181434654.321],
    ['2004-10-19 10:23:54.654321+00', 1098181434654.321],
    ['2004-10-19 10:23:54.654321+00:00', 1098181434654.321],
    ['2004-10-19 10:23:54.654321+02', 1098174234654.321],
    ['2024-12-05 16:38:21.907-05', 1733434701907],
    ['2024-12-05 16:38:21.907-05:30', 1733436501907],
  ])('parse timestamp: %s', (timestamp, result) => {
    // expect(new PreciseDate(timestamp).getTime()).toBe(Math.floor(result));
    expect(timestampToFpMillis(timestamp)).toBe(result);
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
    ['timestamp', 'number'],
    ['timestamptz', 'number'],
    ['timestamp with time zone', 'number'],
    ['timestamp without time zone', 'number'],
    ['bpchar', 'string'],
    ['character', 'string'],
    ['character varying', 'string'],
    ['text', 'string'],
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
