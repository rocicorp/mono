import {expect, test} from 'vitest';
import {h64} from '../../shared/src/hash.ts';
import {downstreamSchema} from './down.ts';
import {
  LAST_POKE_PART_PROTOCOL_VERSION,
  POKE_CHUNK_PROTOCOL_VERSION,
} from './poke.ts';
import {
  MIN_SERVER_SUPPORTED_SYNC_PROTOCOL,
  PROTOCOL_VERSION,
} from './protocol-version.ts';
import {upstreamSchema} from './up.ts';

test('protocol version', () => {
  const schemaJSON = JSON.stringify({upstreamSchema, downstreamSchema});
  const hash = h64(schemaJSON).toString(36);

  // If this test fails upstream or downstream schema has changed such that
  // old code will not understand the new schema, bump the
  // PROTOCOL_VERSION and update the expected values.
  expect(hash).toEqual('24niurwt66lah');
  expect(PROTOCOL_VERSION).toBe(52);
  expect(LAST_POKE_PART_PROTOCOL_VERSION).toBe(51);
  expect(POKE_CHUNK_PROTOCOL_VERSION).toBe(52);
});

test('server support retains the CloudZero protocol floor', () => {
  // CloudZero may continue serving clients at this version indefinitely.
  expect(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL).toBe(30);
});
