import {describe, expect, expectTypeOf, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {
  ApplicationError,
  isApplicationError,
  wrapWithApplicationError,
} from './application-error.ts';

describe('ApplicationError', () => {
  test('creates error with message and object details', () => {
    const error = new ApplicationError('Validation failed', {
      details: {code: 'ERR_001', field: 'email'},
    });
    expect(error.message).toBe('Validation failed');
    expect(error.name).toBe('ApplicationError');
    expect(error.details).toEqual({code: 'ERR_001', field: 'email'});
  });

  test('creates error without details', () => {
    const error = new ApplicationError('Something went wrong');
    expect(error.details).toBeUndefined();
    expectTypeOf(error.details).toEqualTypeOf<ReadonlyJSONValue | undefined>();
  });

  test('creates error with cause', () => {
    const cause = new Error('Original error');
    const error = new ApplicationError('Wrapped error', {
      details: {code: 'ERR_002'},
      cause,
    });
    expect(error.cause).toBe(cause);
    expect(error.details).toEqual({code: 'ERR_002'});
    expectTypeOf(error.details).toEqualTypeOf<{readonly code: 'ERR_002'}>();
  });

  test('supports typed details parameter', () => {
    // Test with specific object type
    type ErrorDetails = {code: string; timestamp: number};
    const error = new ApplicationError<ErrorDetails>('Error', {
      details: {code: 'ERR_003', timestamp: 123456},
    });
    const details: ErrorDetails = error.details;
    expect(details.code).toBe('ERR_003');
    expect(details.timestamp).toBe(123456);
    expectTypeOf(error.details).toEqualTypeOf<ErrorDetails>();
  });

  test('supports ReadonlyJSONValue type parameter', () => {
    const error = new ApplicationError<ReadonlyJSONValue>('Error', {
      details: {nested: {array: [1, 2, 3]}},
    });
    const details: ReadonlyJSONValue = error.details;
    expect(details).toEqual({nested: {array: [1, 2, 3]}});
    expectTypeOf(error.details).toEqualTypeOf<ReadonlyJSONValue>();
  });

  test('supports primitive types as details', () => {
    const stringError = new ApplicationError<string>('Error', {
      details: 'error info',
    });
    const numError = new ApplicationError<number>('Error', {details: 42});

    expect(stringError.details).toBe('error info');
    expect(numError.details).toBe(42);
    expectTypeOf(stringError.details).toEqualTypeOf<string>();
    expectTypeOf(numError.details).toEqualTypeOf<number>();
  });
});

describe('isApplicationError', () => {
  test('returns true for ApplicationError', () => {
    const error = new ApplicationError('Test');
    expect(isApplicationError(error)).toBe(true);
  });

  test('returns false for regular Error', () => {
    expect(isApplicationError(new Error('Test'))).toBe(false);
  });

  test('returns false for non-errors', () => {
    expect(isApplicationError('error string')).toBe(false);
    expect(isApplicationError({message: 'error'})).toBe(false);
    expect(isApplicationError(null)).toBe(false);
  });
});

describe('wrapWithApplicationError', () => {
  test('returns same error if already ApplicationError', () => {
    const error = new ApplicationError('Test', {details: {code: 'ERR_004'}});
    const wrapped = wrapWithApplicationError(error);
    expect(wrapped).toBe(error);
  });

  test('wraps Error with message and details', () => {
    const error = new Error('Original error');
    const wrapped = wrapWithApplicationError(error);
    expect(wrapped.message).toBe('Original error');
    expect(wrapped.cause).toBe(error);
    expect(wrapped.details).toBeUndefined();
  });

  test('wraps Error with custom name', () => {
    const error = new Error('Original error');
    error.name = 'CustomError';
    const wrapped = wrapWithApplicationError(error);
    expect(wrapped.message).toBe('Original error');
    expect(wrapped.details).toEqual({
      name: 'CustomError',
    });
  });

  test('wraps string errors', () => {
    const wrapped = wrapWithApplicationError('error string');
    expect(wrapped.message).toBe('error string');
    expect(wrapped.cause).toBe('error string');
  });

  test('wraps non-Error objects', () => {
    const obj = {code: 'ERR_005', status: 500};
    const wrapped = wrapWithApplicationError(obj);
    expect(wrapped.details).toEqual(obj);
    expect(wrapped.cause).toBe(obj);
  });

  test('handles non-JSON-serializable values', () => {
    const circular = {ref: null as unknown};
    circular.ref = circular;
    const wrapped = wrapWithApplicationError(circular);
    expect(wrapped.details).toBeUndefined();
    expect(wrapped.cause).toBe(circular);
  });
});
