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
  sourceSha: string | undefined;
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
  for (const {version, sourceSha} of matches) {
    log(`${version} -> ${sourceSha ?? '(unknown source commit)'}`);
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
    return entry
      ? [{version: query, sourceSha: sourceShaOf(query, entry)}]
      : [];
  }
  if (shaPrefixPattern.test(query)) {
    return Object.entries(versions)
      .map(([version, entry]) => ({
        version,
        sourceSha: sourceShaOf(version, entry),
      }))
      .filter(({sourceSha}) => matchesShaQuery(sourceSha, query));
  }
  throw new Error(
    `Expected a Zero version, git SHA, or SHA prefix (6+ hex chars), got ${query}`,
  );
}

/**
 * Head versions embed a source-sha prefix in the version string; stable and
 * canary releases are mapped by their `zero/vX.Y.Z` git tags, but a
 * packument-recorded gitHead is honored when present.
 */
function sourceShaOf(
  version: string,
  entry: {gitHead?: string | undefined},
): string | undefined {
  return parseZeroVersion(version)?.headSha ?? entry.gitHead;
}

// A stored sha may be shorter than the query (head versions embed a fixed
// prefix, the query may be a full 40-char sha) or longer (short query
// against a full packument gitHead) — either containment direction matches.
function matchesShaQuery(sourceSha: string | undefined, query: string) {
  return (
    sourceSha !== undefined &&
    (sourceSha.startsWith(query) || query.startsWith(sourceSha))
  );
}

async function readPackumentVersions(
  fetchImpl: MapReleaseFetch,
): Promise<PackumentVersions> {
  // The full packument (not the abbreviated install metadata) carries
  // per-version gitHead when the registry recorded one.
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
