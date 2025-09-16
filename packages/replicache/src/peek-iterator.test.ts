/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {expect, test} from 'vitest';
import {PeekIterator} from './peek-iterator.ts';

test('PeekIterator', () => {
  const c = new PeekIterator('abc'[Symbol.iterator]());
  expect(c.peek().value).to.equal('a');
  expect(c.peek().value).to.equal('a');
  expect(c.next().value).to.equal('a');
  expect(c.peek().value).to.equal('b');
  expect(c.peek().value).to.equal('b');
  expect(c.next().value).to.equal('b');
  expect(c.peek().value).to.equal('c');
  expect(c.peek().value).to.equal('c');
  expect(c.next().value).to.equal('c');
  expect(c.peek().done).to.be.true;
  expect(c.peek().done).to.be.true;
  expect(c.next().done).to.be.true;
});
