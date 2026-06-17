import {expect, test} from 'vitest';
import {h64} from '../../shared/src/hash.ts';
import {downstreamSchema} from './down.ts';
import {PROTOCOL_VERSION} from './protocol-version.ts';
import {upstreamSchema} from './up.ts';

test('protocol version', () => {
  const schemaJSON = JSON.stringify({upstreamSchema, downstreamSchema});
  const hash = h64(schemaJSON).toString(36);

  // This hash is a pessimistic fingerprint of the schema's internal class
  // representation. It can fire on additive refactors (e.g. wrapping the
  // union in a chained validator that rejects pathologically deep inputs)
  // that do not affect what an old client is allowed to send. The wire
  // grammar contract lives in `upstream-schema.test.ts`; if those tests
  // still pass after a schema refactor, the wire format has not changed
  // and updating the hash without bumping PROTOCOL_VERSION is safe.
  //
  // If `upstream-schema.test.ts` *also* needed to be updated (i.e. some
  // existing example was changed or removed, or a new mandatory field was
  // added that breaks an old client), bump PROTOCOL_VERSION and update
  // this hash together.
  expect(hash).toEqual('180ln8s9hw9zw');
  expect(PROTOCOL_VERSION).toBe(51);
});
