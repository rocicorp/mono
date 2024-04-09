import {describe, expect, test} from '@jest/globals';
import pg from 'pg';
import {typeNameByOID} from './pg.js';

describe('types/pg-types', () => {
  test('typeNameByIOD', () => {
    const {
      types: {builtins},
    } = pg;

    expect(typeNameByOID[builtins.BYTEA]).toBe('bytea');
    expect(typeNameByOID[builtins.INT4]).toBe('int4');
    expect(typeNameByOID[builtins.TEXT]).toBe('text');
    expect(typeNameByOID[builtins.VARCHAR]).toBe('varchar');
    expect(typeNameByOID[1007]).toBe('int4[]');

    expect(() => (typeNameByOID[1007] = 'should not work')).toThrowError();
    expect(typeNameByOID[1007]).toBe('int4[]');
  });
});
