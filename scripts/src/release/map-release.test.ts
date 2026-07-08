import {expect, test, vi} from 'vitest';
import {
  findMapReleaseMatches,
  mapRelease,
  npmPackumentUrl,
  type MapReleaseFetch,
  type PackumentVersions,
} from './map-release.ts';

const versions: PackumentVersions = {
  '1.8.0': {gitHead: 'aaaa111111111111111111111111111111111111'},
  '1.8.0-canary.0': {gitHead: 'bbbb222222222222222222222222222222222222'},
  '1.8.0-head.202607082153': {
    gitHead: 'bbbb222222222222222222222222222222222222',
  },
  '1.7.0': {},
};

test('findMapReleaseMatches maps versions to shas and sha prefixes to versions', () => {
  expect(findMapReleaseMatches(versions, '1.8.0')).toEqual([
    {version: '1.8.0', gitHead: 'aaaa111111111111111111111111111111111111'},
  ]);
  expect(findMapReleaseMatches(versions, '1.9.9')).toEqual([]);

  expect(findMapReleaseMatches(versions, 'bbbb22')).toEqual([
    {
      version: '1.8.0-canary.0',
      gitHead: 'bbbb222222222222222222222222222222222222',
    },
    {
      version: '1.8.0-head.202607082153',
      gitHead: 'bbbb222222222222222222222222222222222222',
    },
  ]);
  expect(findMapReleaseMatches(versions, 'cccc33')).toEqual([]);

  expect(() =>
    findMapReleaseMatches(versions, 'zero!'),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Expected a Zero version, git SHA, or SHA prefix (6+ hex chars), got zero!]`,
  );
});

test('mapRelease prints npm and image references for matches', async () => {
  const log = vi.fn();
  const fetchImpl: MapReleaseFetch = url => {
    expect(url).toBe(npmPackumentUrl);
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => Promise.resolve({versions}),
    });
  };

  await expect(mapRelease({fetchImpl, log, query: '1.8.0'})).resolves.toEqual([
    {version: '1.8.0', gitHead: 'aaaa111111111111111111111111111111111111'},
  ]);

  expect(log.mock.calls.map(([line]) => line)).toEqual([
    '1.8.0 -> aaaa111111111111111111111111111111111111',
    '  npm:   @rocicorp/zero@1.8.0',
    '  image: ghcr.io/rocicorp/zero:1.8.0',
  ]);
});

test('mapRelease reports versions without a recorded gitHead', async () => {
  const log = vi.fn();
  const fetchImpl: MapReleaseFetch = () =>
    Promise.resolve({
      ok: true,
      status: 200,
      json: () => Promise.resolve({versions}),
    });

  await mapRelease({fetchImpl, log, query: '1.7.0'});

  expect(log).toHaveBeenCalledWith('1.7.0 -> (no gitHead recorded)');
});

test('mapRelease throws when nothing matches or the registry fails', async () => {
  const fetchImpl: MapReleaseFetch = () =>
    Promise.resolve({
      ok: true,
      status: 200,
      json: () => Promise.resolve({versions}),
    });

  await expect(
    mapRelease({fetchImpl, log: vi.fn(), query: 'cccc333333'}),
  ).rejects.toThrowErrorMatchingInlineSnapshot(
    `[Error: No @rocicorp/zero release found for cccc333333]`,
  );

  const failingFetch: MapReleaseFetch = () =>
    Promise.resolve({
      ok: false,
      status: 503,
      json: () => Promise.reject(new Error('unreachable')),
    });
  await expect(
    mapRelease({fetchImpl: failingFetch, log: vi.fn(), query: '1.8.0'}),
  ).rejects.toThrow(/Could not read .*: HTTP 503/);
});
