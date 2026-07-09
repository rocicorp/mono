import {execFileSync} from 'node:child_process';
import {appendFileSync, readFileSync, writeFileSync} from 'node:fs';

export const zeroPackageName = '@rocicorp/zero';
export const zeroPackageJsonPath = 'packages/zero/package.json';
export const zeroGhcrImage = 'ghcr.io/rocicorp/zero';

export type Command = 'docker' | 'git' | 'npm' | 'tar';
export type ExecOptions = {
  cwd?: string | undefined;
  env?: NodeJS.ProcessEnv | undefined;
  input?: string | undefined;
  stdio?: 'pipe' | 'inherit' | 'ignore' | ['ignore', 'pipe', 'pipe'];
};
export type Exec = (
  command: Command,
  args: readonly string[],
  options?: ExecOptions,
) => string;
export type ExecError = Error & {status?: number | undefined; stderr?: Buffer};

export type StableZeroVersion = `${number}.${number}.${number}`;
export type CanaryZeroVersion = `${StableZeroVersion}-canary.${number}`;
export type HeadZeroVersion = `${StableZeroVersion}-head-${string}`;
export type ZeroVersion =
  | StableZeroVersion
  | CanaryZeroVersion
  | HeadZeroVersion;
export type ZeroTag<V extends ZeroVersion = ZeroVersion> = `zero/v${V}`;
export type MaintenanceZeroBranch = `maint/zero/v${number}.${number}`;
export type ReleaseBranch = 'main' | MaintenanceZeroBranch;

export const defaultExec: Exec = (command, args, options) =>
  String(
    execFileSync(command, [...args], {
      encoding: 'utf8',
      ...options,
    }) ?? '',
  );

const stableZeroVersionPattern = /^\d+\.\d+\.\d+$/;
/**
 * Length of the source-sha prefix embedded in head versions. Eight hex chars
 * keep short-sha lookups (git, GitHub) unambiguous at this repo's size; six
 * would collide with other objects a few percent of the time.
 *
 * The whole head suffix is joined with hyphens so it stays a single semver
 * alphanumeric prerelease identifier: dot-separated segments would make an
 * all-digit sha prefix with a leading zero (e.g. `.012345.`) invalid semver.
 */
export const headVersionShaLength = 8;
const zeroVersionPattern = new RegExp(
  `^(\\d+\\.\\d+\\.\\d+)(?:-canary\\.\\d+|-head-([0-9a-f]{${headVersionShaLength}})-\\d{8})?$`,
);
const gitShaPattern = /^[0-9a-f]{40}$/;

export type ReleaseMode = 'canary' | 'stable' | 'head';

export function mustEnv(name: string) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

export function writeGithubOutput(outputs: Record<string, string>) {
  const githubOutput = mustEnv('GITHUB_OUTPUT');
  appendFileSync(
    githubOutput,
    Object.entries(outputs)
      .map(([key, value]) => {
        if (value.includes('\n') || value.includes('\r')) {
          throw new Error(`Output ${key} contains a newline`);
        }
        return `${key}=${value}\n`;
      })
      .join(''),
  );
}

export function readReleaseMode(mode: string): ReleaseMode {
  if (mode !== 'canary' && mode !== 'stable' && mode !== 'head') {
    throw new Error(`Unsupported release mode ${mode}`);
  }
  return mode;
}

export function assertMainWorkflowRef(workflowName: string, refName: string) {
  if (refName !== 'main') {
    throw new Error(
      `${workflowName} workflow must be run from main, got ${refName}`,
    );
  }
}

export function assertGitSha(value: string, name: string) {
  if (!gitShaPattern.test(value)) {
    throw new Error(`Invalid ${name} ${value}`);
  }
}

export function assertZeroVersion(
  version: string,
): asserts version is ZeroVersion {
  if (!zeroVersionPattern.test(version)) {
    throw new Error(`Invalid version ${version}`);
  }
}

export function assertStableZeroVersion(
  version: string,
  message?: string,
): asserts version is StableZeroVersion {
  if (!stableZeroVersionPattern.test(version)) {
    throw new Error(message ?? `Expected stable Zero version, got ${version}`);
  }
}

export function parseZeroVersion(version: string):
  | {
      baseVersion: StableZeroVersion;
      headSha: string | undefined;
    }
  | undefined {
  const match = version.match(zeroVersionPattern);
  if (!match) {
    return undefined;
  }
  return {
    baseVersion: match[1] as StableZeroVersion,
    headSha: match[2],
  };
}

export function zeroTag<V extends ZeroVersion>(version: V): ZeroTag<V> {
  return `zero/v${version}` as ZeroTag<V>;
}

export function assertZeroTagMatchesVersion<V extends ZeroVersion>(
  tag: string,
  version: V,
): asserts tag is ZeroTag<V> {
  if (tag !== zeroTag(version)) {
    throw new Error(`Tag ${tag} does not match version ${version}`);
  }
}

export function readZeroPackageVersionAt(
  sourceSha: string,
  exec = defaultExec,
): ZeroVersion {
  const packageJson = JSON.parse(
    exec('git', ['show', `${sourceSha}:${zeroPackageJsonPath}`]),
  ) as {version?: unknown};
  if (typeof packageJson.version !== 'string') {
    throw new Error(`${zeroPackageJsonPath} has no string version`);
  }
  assertZeroVersion(packageJson.version);
  return packageJson.version;
}

export function writeZeroPackageVersion(
  version: ZeroVersion,
  packageJsonPath = zeroPackageJsonPath,
) {
  const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf8')) as {
    version?: string;
  };
  const previousVersion = packageJson.version;
  packageJson.version = version;
  writeFileSync(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`);
  return previousVersion;
}

export function gitTagExists(tag: ZeroTag, exec = defaultExec) {
  try {
    exec('git', ['rev-parse', '--verify', '--quiet', `refs/tags/${tag}`], {
      stdio: 'ignore',
    });
    return true;
  } catch {
    return false;
  }
}

export function assertGitTagExists(tag: ZeroTag, exec = defaultExec) {
  if (!gitTagExists(tag, exec)) {
    throw new Error(`Git tag ${tag} does not exist`);
  }
}

export function npmZeroVersionExists(version: ZeroVersion, exec = defaultExec) {
  try {
    return readNpmZeroVersion(version, exec).length > 0;
  } catch (error) {
    const {status, stderr: stderrBuffer} = error as ExecError;
    const stderr = String(stderrBuffer ?? '');
    if (
      stderr.includes('E404') ||
      stderr.includes('404 Not Found') ||
      (status === 1 && stderr.trim().length === 0)
    ) {
      return false;
    }
    throw new Error(
      `Could not check npm for ${zeroPackageName}@${version}: ${stderr}`,
    );
  }
}

export function assertNpmZeroVersionExists(
  version: ZeroVersion,
  exec = defaultExec,
) {
  const output = readNpmZeroVersion(version, exec);
  if (output !== version) {
    throw new Error(`npm resolved ${output}, expected ${version}`);
  }
}

export function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function readNpmZeroVersion(version: ZeroVersion, exec: Exec) {
  return exec(
    'npm',
    ['view', '--silent', `${zeroPackageName}@${version}`, 'version'],
    {stdio: ['ignore', 'pipe', 'pipe']},
  ).trim();
}
