import type {UserAgent} from 'mirror-protocol/src/user-agent.js';
import {
  DistTag,
  DistTagMap,
  lookupDistTags,
} from 'mirror-protocol/src/version.js';
import {readFileSync} from 'node:fs';
import color from 'picocolors';
import {Range, SemVer, gt, gtr} from 'semver';
import type {ArgumentsCamelCase} from 'yargs';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {getLogger} from './logger.js';

declare const REFLECT_VERSION: string;
declare const REFLECT_CLI_NAME: string;

export const version = findReflectVersion();

export function getUserAgent(): UserAgent {
  return {
    type: REFLECT_CLI_NAME,
    version: findReflectVersion(),
  };
}

type DistTags = DistTagMap<SemVer>;

// Run by yargs middleware. Stashes the DistTags in argv so that `npm view` is only run once.
export async function tryDeprecationCheck(
  argv: ArgumentsCamelCase,
): Promise<void> {
  try {
    // Store the DistTags in argv so that it can be checked again if necessary.
    argv._distTags = await checkForCliDeprecation();
  } catch (e) {
    console.warn(`Unable to check for deprecation. Proceeding anyway.`);
  }
}

/**
 * Extracts the DistTags stored in yargs during the deprecation check. In the case
 * that the deprecation check failed, this will attempt to fetch the DistTags again
 * and throw if still unsuccessful.
 */
function getOrRefetchDistTags(
  yargs: YargvToInterface<CommonYargsArgv>,
): Promise<DistTags> {
  if (yargs._distTags) {
    return Promise.resolve(yargs._distTags as DistTags);
  }
  // Use a longer, 30 second timeout when DistTags are needed to proceed.
  return lookupDistTags(SemVer, 30000);
}

export function findReflectVersion(): string {
  if (typeof REFLECT_VERSION === 'string') {
    return REFLECT_VERSION;
  }

  // When the reflect-cli is run from source, use the version from `packages/reflect/package.json`.
  const url = new URL(
    '../../../packages/reflect/package.json',
    import.meta.url,
  );
  return JSON.parse(readFileSync(url, 'utf-8')).version;
}

async function checkForCliDeprecation(): Promise<DistTags> {
  // Use a short, 3 second timeout to reduce delays if machine is offline (e.g. `reflect dev`).
  const versions = await lookupDistTags(SemVer, 3000);
  const minSupported = versions[DistTag.MinSupported];
  const minNonDeprecated = versions[DistTag.MinNonDeprecated];
  const latest = versions[DistTag.Latest];
  const current = new SemVer(findReflectVersion());
  if (minSupported && gt(minSupported, current)) {
    notifyUnsupported();
  } else if (minNonDeprecated && gt(minNonDeprecated, current)) {
    notifyDeprecated();
  } else if (latest && gt(latest, current)) {
    notifyLatest(latest, current.raw);
  }
  return versions;
}

export async function checkForServerDeprecation(
  yargs: YargvToInterface<CommonYargsArgv>,
  serverVersionRange: Range,
): Promise<void> {
  const versions = await getOrRefetchDistTags(yargs);
  const minSupported = versions[DistTag.MinSupported];
  const minNonDeprecated = versions[DistTag.MinNonDeprecated];
  const latest = versions[DistTag.Latest];
  if (minSupported && gtr(minSupported, serverVersionRange)) {
    notifyUnsupported();
  } else if (minNonDeprecated && gtr(minNonDeprecated, serverVersionRange)) {
    notifyDeprecated();
  } else if (latest && gtr(latest, serverVersionRange)) {
    notifyLatest(latest, serverVersionRange.raw);
  }
}

function notifyUnsupported() {
  getLogger().error(
    `${color.red('This version of Reflect is no longer supported.')}\n` +
      `Please update to ${color.bold('@rocicorp/reflect@latest')}.\n`,
  );
  process.exit(-1);
}

function notifyDeprecated() {
  getLogger().error(
    `${color.yellow(
      'Note: This version of Reflect is deprecated and will stop working soon.',
    )}\n` + `Please update to ${color.bold('@rocicorp/reflect@latest')}.\n`,
  );
}

function notifyLatest(latest: SemVer, current: string) {
  getLogger().error(
    `${color.green(
      `Tip: Reflect ${latest.version} is now available. Version ${current} is out of date.`,
    )}\n` +
      `For the latest features, update to ${color.bold(
        '@rocicorp/reflect@latest',
      )}.\n`,
  );
}
