import {describe, expect, test} from 'vitest';
import {encodeSecProtocols} from '../../../zero-protocol/src/connect.ts';
import {getConnectParams} from './connect-params.ts';

function affinityKeyOf(extraParams: string) {
  const url = new URL(
    `http://unused/sync/v57/connect?clientID=c&clientGroupID=g&ts=1&lmid=0${extraParams}`,
  );
  const {params, error} = getConnectParams(57, url, {
    'sec-websocket-protocol': encodeSecProtocols(undefined, undefined),
  });
  expect(error).toBeNull();
  return params?.affinityKey;
}

describe('affinityKey', () => {
  test('uses the explicit key', () => {
    expect(affinityKeyOf('&userID=alice&affinityKey=team')).toBe('team');
  });

  test('falls back to the userID sent by an older client', () => {
    expect(affinityKeyOf('&userID=alice')).toBe('alice');
  });

  test('is undefined with neither', () => {
    expect(affinityKeyOf('')).toBeUndefined();
  });
});
