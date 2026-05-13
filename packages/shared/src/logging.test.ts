import {expect, test} from 'vitest';
import {AbortError} from './abort-error';
import type {ReadonlyJSONValue} from './json';
import {errorOrObject} from './logging';

class CustomErrorWithDetails extends Error {
  readonly details: ReadonlyJSONValue;

  constructor(message: string) {
    super(message);
    this.name = 'CustomErrorWithDetails';
    this.details = {some: 'details'};
  }
}

test('errorOrObject', () => {
  expect(errorOrObject({foo: 'bar'})).toMatchObject({
    foo: 'bar',
  });

  expect(errorOrObject(new Error('foo'))).toMatchObject({
    errorMsg: 'foo',
    name: 'Error',
    stack: /Error: foo.*/,
  });

  expect(errorOrObject(new CustomErrorWithDetails('foo'))).toMatchObject({
    errorMsg: 'foo',
    name: 'CustomErrorWithDetails',
    details: {some: 'details'},
    stack: /CustomErrorWithDetails: foo.*/,
  });

  expect(
    errorOrObject(
      new AbortError('foo', {
        cause: new Error('bar', {cause: new CustomErrorWithDetails('baz')}),
      }),
    ),
  ).toMatchObject({
    errorMsg: 'foo',
    name: 'AbortError',
    stack: /AbortError: foo.*/,
    cause: {
      errorMsg: 'bar',
      name: 'Error',
      stack: /Error: bar.*/,
      cause: {
        errorMsg: 'baz',
        name: 'CustomErrorWithDetails',
        details: {some: 'details'},
        stack: /CustomErrorWithDetails: baz.*/,
      },
    },
  });
});
