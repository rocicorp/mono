import {test, expect} from '@jest/globals';
import {encodeReason} from 'shared/src/cf/socket.js';

test('encodeReason', () => {
  expect(encodeReason('')).toEqual('');
  expect(encodeReason('foo')).toEqual('foo');

  {
    const res = encodeReason('s'.repeat(500));
    expect(res.length).toEqual(123);
    expect(res).toEqual('s'.repeat(120) + '...');
  }
  {
    const msg = 's'.repeat(123);
    expect(encodeReason(msg)).toEqual(msg);
  }
  {
    const msg = 's'.repeat(124);
    expect(encodeReason(msg)).toEqual(msg.slice(0, 120) + '...');
  }
  {
    const msg = 's'.repeat(125);
    expect(encodeReason(msg)).toEqual(msg.slice(0, 120) + '...');
  }
  {
    const msg = 's'.repeat(126);
    expect(encodeReason(msg)).toEqual(msg.slice(0, 120) + '...');
  }

  expect(encodeReason('a💩b👻c')).toEqual('a?b?c');

  expect(encodeReason('123€')).toEqual('123?');
});
