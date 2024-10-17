import {expect, test} from 'vitest';
import {newRequestID} from './request-id.js';

test('newRequestID()', () => {
  {
    const re = /client-[0-9a-f]+-0$/;
    const got = newRequestID('client');
    expect(got).to.match(re);
  }
  {
    const re = /client-[0-9a-f]+-0$/;
    const got = newRequestID('client');
    expect(got).to.match(re);
  }
});
