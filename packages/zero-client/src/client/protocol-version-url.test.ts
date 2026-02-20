import {expect, test, vi} from 'vitest';
import {h64} from '../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {stringCompare} from '../../../shared/src/string-compare.ts';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.ts';
import {createConnectionURL} from './zero.ts';

test('When the URL changes we need to update the protocol version', async () => {
  vi.spyOn(performance, 'now').mockReturnValue(123456);

  const lc = createSilentLogContext();
  const url = await createConnectionURL(
    'https://example.com' as const,
    'client-id',
    'client-group-id',
    'user-id',
    'base-cookie',
    123,
    'wsid',
    {profileID: Promise.resolve('profile-id')},
    false,
    {additional: 'connect', param: 's'},
    lc,
  );

  // The order of query parameters does not matter. Sort them before hashing.
  const sorted = new URLSearchParams(
    [...url.searchParams.entries()].sort((a, b) => stringCompare(a[0], b[0])),
  );

  const hash = h64(sorted.toString()).toString(36);

  // If this test fails the URL search params have changed such that old code
  // might not understand it. You may need to bump the PROTOCOL_VERSION and
  // update the expected values.
  expect(hash).toBe('10r2tfvet9a3s');
  expect(PROTOCOL_VERSION).toBe(49);
});
