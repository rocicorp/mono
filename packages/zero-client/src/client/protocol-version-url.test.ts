import {expect, test, vi} from 'vitest';
import {h64} from '../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {stringCompare} from '../../../shared/src/string-compare.ts';
import {CLIENT_PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.ts';
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
    // oxlint-disable-next-line e18e/prefer-array-to-sorted
    [...url.searchParams.entries()].sort((a, b) => stringCompare(a[0], b[0])),
  );

  const hash = h64(sorted.toString()).toString(36);

  // If this test fails the URL search params have changed. Old servers ignore
  // parameters they don't know, so a new optional parameter or feature flag
  // is fine. A change old servers can't ignore needs a CLIENT_PROTOCOL_VERSION
  // bump (see protocol-version.ts). Then update the expected values.
  expect(hash).toBe('30nqhuku9q5i7');
  expect(CLIENT_PROTOCOL_VERSION).toBe(51);
  expect(url.pathname).toBe(`/sync/v51/connect`);
});
