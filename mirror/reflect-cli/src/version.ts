// TODO(arv): Use esbuild define instead.
import packageJSON from '../package.json' assert {type: 'json'};
import color from 'picocolors';
import {Range, SemVer, gt, gtr} from 'semver';
import type {ArgumentsCamelCase} from 'yargs';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {assert, assertObject, assertString} from 'shared/src/asserts.js';
import {pkgUp} from 'pkg-up';
import {readFile} from 'node:fs/promises';

export const {version} = packageJSON;

export const userAgent = {
  type: packageJSON.name,
  version,
} as const;

export type UserAgent = typeof userAgent;

const enum DistTag {
  Latest = 'latest',
  MinSupported = 'sup',
  MinNonDeprecated = 'rec',
}

type DistTags = {[tag: string]: SemVer};

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
  return lookupDistTags(30000);
}

async function lookupDistTags(timeout: number): Promise<DistTags> {
  // https://github.com/npm/registry/blob/master/docs/REGISTRY-API.md
  const resp = await fetch('https://registry.npmjs.org/@rocicorp/reflect', {
    signal: AbortSignal.timeout(timeout),
  });
  const pkgMeta = await resp.json();
  const distTags = pkgMeta['dist-tags'] as Record<string, string>;
  return Object.fromEntries(
    Object.entries(distTags).map(([tag, value]) => [tag, new SemVer(value)]),
  );
}

export async function findReflectVersion(): Promise<string> {
  const pkgDir = fileURLToPath(import.meta.url);
  if (!pkgDir.includes('/node_module')) {
    const reflectPkg = path.resolve(pkgDir, '../../../../', 'packages/reflect');
    const pkg = await pkgUp({cwd: reflectPkg});
    assert(pkg);
    const s = await readFile(pkg, 'utf-8');
    const v = JSON.parse(s);
    assertObject(v);
    assertString(v.version);
    console.log(
      `reflect-cli run from source. Using version from packages/reflect/package.json: ${v.version}.`,
    );
    return v.version;
  }
  return version;
}

async function checkForCliDeprecation(): Promise<DistTags> {
  // Use a short, 3 second timeout to reduce delays if machine is offline (e.g. `reflect dev`).
  const versions = await lookupDistTags(3000);
  const minSupported = versions[DistTag.MinSupported];
  const minNonDeprecated = versions[DistTag.MinNonDeprecated];
  const latest = versions[DistTag.Latest];
  const current = new SemVer(await findReflectVersion());
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
  console.error(
    `${color.red('This version of Reflect is no longer supported.')}\n` +
      `Please update to ${color.bold('@rocicorp/reflect@latest')}.\n`,
  );
  process.exit(-1);
}

function notifyDeprecated() {
  console.error(
    `${color.yellow(
      'Note: This version of Reflect is deprecated and will stop working soon.',
    )}\n` + `Please update to ${color.bold('@rocicorp/reflect@latest')}.\n`,
  );
}

function notifyLatest(latest: SemVer, current: string) {
  console.error(
    `${color.green(
      `Tip: Reflect ${latest.version} is now available. Version ${current} is out of date.`,
    )}\n` +
      `For the latest features, update to ${color.bold(
        '@rocicorp/reflect@latest',
      )}.\n`,
  );
}
