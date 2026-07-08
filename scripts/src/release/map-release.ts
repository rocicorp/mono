// oxlint-disable no-console

import {parseZeroVersion, zeroGhcrImage, zeroPackageName} from '../shared.ts';

export const npmPackumentUrl = `https://registry.npmjs.org/${zeroPackageName.replace('/', '%2F')}`;

const shaPrefixPattern = /^[0-9a-f]{6,40}$/;

export type PackumentVersions = Record<string, {gitHead?: string | undefined}>;

export type MapReleaseFetch = (
  url: string,
  init: {headers: Record<string, string>},
) => Promise<{ok: boolean; status: number; json(): Promise<unknown>}>;

export type MapReleaseMatch = {
  version: string;
  gitHead: string | undefined;
};

export type MapReleaseOptions = {
  fetchImpl?: MapReleaseFetch | undefined;
  log?: ((message: string) => void) | undefined;
  query: string;
};

export async function runMapReleaseCli() {
  const query = process.argv[2];
  if (!query) {
    throw new Error('Usage: map-release <version|sha|sha-prefix>');
  }
  await mapRelease({query});
}

export async function mapRelease({
  fetchImpl = fetch,
  log = console.log,
  query,
}: MapReleaseOptions): Promise<MapReleaseMatch[]> {
  const versions = await readPackumentVersions(fetchImpl);
  const matches = findMapReleaseMatches(versions, query);
  if (matches.length === 0) {
    throw new Error(`No ${zeroPackageName} release found for ${query}`);
  }
  for (const {version, gitHead} of matches) {
    log(`${version} -> ${gitHead ?? '(no gitHead recorded)'}`);
    log(`  npm:   ${zeroPackageName}@${version}`);
    log(`  image: ${zeroGhcrImage}:${version}`);
  }
  return matches;
}

export function findMapReleaseMatches(
  versions: PackumentVersions,
  query: string,
): MapReleaseMatch[] {
  if (parseZeroVersion(query)) {
    const entry = versions[query];
    return entry ? [{version: query, gitHead: entry.gitHead}] : [];
  }
  if (shaPrefixPattern.test(query)) {
    return Object.entries(versions)
      .filter(([, {gitHead}]) => gitHead?.startsWith(query))
      .map(([version, {gitHead}]) => ({version, gitHead}));
  }
  throw new Error(
    `Expected a Zero version, git SHA, or SHA prefix (6+ hex chars), got ${query}`,
  );
}

async function readPackumentVersions(
  fetchImpl: MapReleaseFetch,
): Promise<PackumentVersions> {
  // The full packument (not the abbreviated install metadata) is required;
  // only the full document carries per-version gitHead.
  const response = await fetchImpl(npmPackumentUrl, {
    headers: {accept: 'application/json'},
  });
  if (!response.ok) {
    throw new Error(
      `Could not read ${npmPackumentUrl}: HTTP ${response.status}`,
    );
  }
  const packument = (await response.json()) as {
    versions?: PackumentVersions | undefined;
  };
  return packument.versions ?? {};
}
