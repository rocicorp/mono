import {expect, test} from 'vitest';
import {h64} from '../../shared/src/hash.ts';
import {downstreamSchema} from './down.ts';
import {PROTOCOL_VERSION} from './protocol-version.ts';
import {upstreamSchema} from './up.ts';

test('protocol version', () => {
  const schemaJSON = JSON.stringify({upstreamSchema, downstreamSchema});
  const hash = h64(schemaJSON).toString(36);

  // If this test fails upstream or downstream schema has changed such that
  // old code will not understand the new schema, bump the
  // PROTOCOL_VERSION and update the expected values.
  expect(hash).toEqual('145xgagl5c356');
  expect(PROTOCOL_VERSION).toBe(45);
});
