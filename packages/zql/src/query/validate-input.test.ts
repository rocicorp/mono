import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, test} from 'vitest';
import {validateInput} from './validate-input.ts';

describe('validateInput', () => {
  test('should return input as-is when no validator is provided', () => {
    const input = {foo: 'bar'};
    const result = validateInput('testQuery', input, undefined, 'query');
    expect(result).toBe(input);
  });

  test('should return validated output when validator succeeds', () => {
    const validator: StandardSchemaV1<number, string> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (data: unknown) => {
          if (typeof data === 'number') {
            return {value: String(data * 2)};
          }
          return {issues: [{message: 'Expected number'}]};
        },
      },
    };

    const result = validateInput('testQuery', 42, validator, 'query');
    expect(result).toBe('84');
  });

  test('should throw error when validation fails', () => {
    const validator: StandardSchemaV1<number, number> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (data: unknown) => {
          if (typeof data === 'number' && data > 0) {
            return {value: data};
          }
          return {issues: [{message: 'Expected positive number'}]};
        },
      },
    };

    expect(() => validateInput('testQuery', -5, validator, 'query')).toThrow(
      'Validation failed for query testQuery: Expected positive number',
    );
  });

  test('should throw error with multiple validation issues', () => {
    const validator: StandardSchemaV1<string, string> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => ({
          issues: [
            {message: 'Too short'},
            {message: 'Invalid format'},
            {message: 'Contains illegal characters'},
          ],
        }),
      },
    };

    expect(() => validateInput('testQuery', 'bad', validator, 'query')).toThrow(
      'Validation failed for query testQuery: Too short, Invalid format, Contains illegal characters',
    );
  });

  test('should throw error when async validator is used', () => {
    const validator: StandardSchemaV1<string, string> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => Promise.resolve({value: 'test'}),
      },
    };

    expect(() =>
      validateInput('testQuery', 'test', validator, 'query'),
    ).toThrow('Async validators are not supported. Query name testQuery');
  });

  test('should use proper title case for mutator kind', () => {
    const validator: StandardSchemaV1<string, string> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => Promise.resolve({value: 'test'}),
      },
    };

    expect(() =>
      validateInput('testMutator', 'test', validator, 'mutator'),
    ).toThrow('Async validators are not supported. Mutator name testMutator');
  });

  test('should handle undefined input', () => {
    const result = validateInput('testQuery', undefined, undefined, 'query');
    expect(result).toBeUndefined();
  });

  test('should validate and transform undefined with validator', () => {
    const validator: StandardSchemaV1<undefined, undefined> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (data: unknown) => {
          if (data === undefined) {
            return {value: undefined};
          }
          return {issues: [{message: 'Expected undefined'}]};
        },
      },
    };

    const result = validateInput('testQuery', undefined, validator, 'query');
    expect(result).toBeUndefined();
  });
});
