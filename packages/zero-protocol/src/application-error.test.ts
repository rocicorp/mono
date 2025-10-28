import {describe, expect, expectTypeOf, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {
  ApplicationError,
  getErrorDetails,
  getErrorMessage,
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
    expectTypeOf(error.details).toEqualTypeOf<undefined>();
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

describe('getErrorMessage', () => {
  test('extracts message from Error', () => {
    expect(getErrorMessage(new Error('Test error'))).toBe('Test error');
  });

  test('returns string directly', () => {
    expect(getErrorMessage('String error')).toBe('String error');
  });

  test('returns fallback for non-string/Error types', () => {
    expect(getErrorMessage(42)).toContain('Type number was thrown');
    expect(getErrorMessage({code: 'ERR'})).toContain('Type object was thrown');
  });
});

describe('getErrorDetails', () => {
  test('returns details from ApplicationError', () => {
    const details = {code: 'ERR_006'};
    const error = new ApplicationError('Test', {details});
    expect(getErrorDetails(error)).toEqual(details);
  });

  test('returns undefined for ApplicationError without details', () => {
    expect(getErrorDetails(new ApplicationError('Test'))).toBeUndefined();
  });

  test('returns name for Error with custom name', () => {
    const error = new Error('Test error');
    error.name = 'CustomError';
    expect(getErrorDetails(error)).toEqual({name: 'CustomError'});
  });

  test('returns undefined for plain Error', () => {
    const error = new Error('Test');
    error.name = 'Error';
    expect(getErrorDetails(error)).toBeUndefined();
  });

  test('parses JSON-serializable values', () => {
    expect(getErrorDetails({code: 'ERR_007'})).toEqual({code: 'ERR_007'});
    expect(getErrorDetails('string')).toBe('string');
    expect(getErrorDetails(42)).toBe(42);
  });

  test('returns undefined for non-JSON-serializable values', () => {
    const circular = {ref: null as unknown};
    circular.ref = circular;
    expect(getErrorDetails(circular)).toBeUndefined();
  });
});
