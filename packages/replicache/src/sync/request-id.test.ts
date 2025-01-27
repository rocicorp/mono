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
