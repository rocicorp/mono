import {describe, expect, test} from 'vitest';

import {getErrorDetails, getErrorMessage} from './error.ts';
import type {ReadonlyJSONValue} from './json.ts';

const UNKNOWN_ERROR_MESSAGE =
  'Unknown error of type object was thrown and the message could not be determined. See cause for details.';

class CustomError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CustomError';
  }
}

class CustomErrorWithDetails extends Error {
  readonly details: ReadonlyJSONValue;

  constructor(message: string) {
    super(message);
    this.name = 'CustomErrorWithDetails';
    this.details = {some: 'details'};
  }
}

describe('getErrorMessage', () => {
  test('returns message from Error instance', () => {
    const error = new Error('boom');
    expect(getErrorMessage(error)).toBe('boom');
  });

  test('returns string input unchanged', () => {
    expect(getErrorMessage('plain string error')).toBe('plain string error');
  });

  test('returns message from CustomError instance', () => {
    const error = new CustomError('custom');
    expect(getErrorMessage(error)).toBe('custom');
  });

  test('stringifies JSON-serializable values', () => {
    const value = {foo: 'bar', nested: [1, 2, {baz: null}]};
    expect(getErrorMessage(value)).toBe(
      `Parsed message: ${JSON.stringify(value)}`,
    );
  });

  test('returns fallback message for non-JSON values', () => {
    expect(getErrorMessage(Symbol('not-json'))).toBe(
      'Unknown error of type symbol was thrown and the message could not be determined. See cause for details.',
    );
  });

  test('returns message from error cause when error has no message', () => {
    const cause = new Error('root cause');
    const error = new Error('', {cause});
    expect(getErrorMessage(error)).toBe('root cause');
  });

  test('returns message from nested error cause', () => {
    const rootCause = new Error('deep error');
    const middleCause = new Error('', {cause: rootCause});
    const error = new Error('', {cause: middleCause});
    expect(getErrorMessage(error)).toBe('deep error');
  });

  test('handles circular error references', () => {
    const error1: Error & {cause?: unknown} = new Error('error1');
    const error2: Error & {cause?: unknown} = new Error('', {cause: error1});
    error1.cause = error2;
    expect(getErrorMessage(error1)).toBe('error1');
  });

  test('detects circular error chain without message', () => {
    const error1: Error & {cause?: unknown} = new Error('');
    const error2: Error & {cause?: unknown} = new Error('', {cause: error1});
    const error3: Error & {cause?: unknown} = new Error('', {cause: error2});
    error1.cause = error3; // Create circular chain
    expect(getErrorMessage(error1)).toBe(
      'Circular error reference detected while extracting the error message.',
    );
  });

  test('handles error with undefined cause', () => {
    expect(getErrorMessage(new Error('', {cause: undefined}))).toBe(
      'Parsed message: {}',
    );
  });

  test('handles objects with message property', () => {
    expect(getErrorMessage({message: 'object error'})).toBe('object error');

    const emptyMessage = {message: ''};
    expect(getErrorMessage(emptyMessage)).toBe(
      `Parsed message: ${JSON.stringify(emptyMessage)}`,
    );

    const nonStringMessage = {message: 123};
    expect(getErrorMessage(nonStringMessage)).toBe(
      `Parsed message: ${JSON.stringify(nonStringMessage)}`,
    );
  });

  test('detects circular references in plain objects', () => {
    const obj: {self?: unknown} = {};
    obj.self = obj;
    expect(getErrorMessage(obj)).toBe(UNKNOWN_ERROR_MESSAGE);
  });
});

describe('getErrorDetails', () => {
  test('returns custom error name', () => {
    const error = new CustomError('custom');
    expect(getErrorDetails(error)).toEqual({name: 'CustomError'});
  });

  test('returns undefined for default Error', () => {
    expect(getErrorDetails(new Error('default'))).toBeUndefined();
  });

  test('prefers structured details property', () => {
    const withDetails = {details: {code: 'ERR_123', retryable: false}};
    expect(getErrorDetails(withDetails)).toEqual({
      code: 'ERR_123',
      retryable: false,
    });
  });

  test('returns details from CustomErrorWithDetails instance', () => {
    const error = new CustomErrorWithDetails('custom');
    expect(getErrorDetails(error)).toEqual({some: 'details'});
  });

  test('falls back to parsing the value itself', () => {
    const jsonValue = {code: 'ERR_FALLBACK', info: ['a', 'b']};
    expect(getErrorDetails(jsonValue)).toEqual(jsonValue);
  });

  test('handles primitive JSON values', () => {
    expect(getErrorDetails('string detail')).toBe('string detail');
    expect(getErrorDetails(42)).toBe(42);
    expect(getErrorDetails(true)).toBe(true);
    expect(getErrorDetails(false)).toBe(false);
    expect(getErrorDetails(null)).toBe(null);
    expect(getErrorDetails(undefined)).toBeUndefined();
  });

  test('returns undefined for non-JSON values', () => {
    expect(getErrorDetails(Symbol('nope'))).toBeUndefined();

    const obj = {details: Symbol('invalid')};
    expect(getErrorDetails(obj)).toBeUndefined();
  });

  test('falls back to error name when details are non-JSON', () => {
    class ErrorWithInvalidDetails extends Error {
      readonly details = Symbol('invalid');

      constructor(message: string) {
        super(message);
        this.name = 'ErrorWithInvalidDetails';
      }
    }
    expect(getErrorDetails(new ErrorWithInvalidDetails('test'))).toEqual({
      name: 'ErrorWithInvalidDetails',
    });
  });
});
