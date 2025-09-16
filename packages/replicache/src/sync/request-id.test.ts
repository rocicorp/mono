/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {expect, test} from 'vitest';
import {newRequestID} from './request-id.ts';

test('newRequestID()', () => {
  {
    const re = /client-[0-9a-f]+-0$/;
    const got = newRequestID('client');
    expect(got).to.match(re);
  }
  {
    const re = /client-[0-9a-f]+-1$/;
    const got = newRequestID('client');
    expect(got).to.match(re);
  }
});

test('make sure we get new IDs every time', () => {
  const clientID = Math.random().toString(36).slice(2);
  const id1 = newRequestID(clientID);
  const id2 = newRequestID(clientID);
  expect(id1).not.toBe(id2);
});
