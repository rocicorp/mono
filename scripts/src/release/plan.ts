// oxlint-disable no-console

import {
  assertGitSha,
  assertMainWorkflowRef,
  assertStableZeroVersion,
  defaultExec,
  escapeRegExp,
  gitTagExists,
  headVersionShaLength,
  mustEnv,
  npmZeroVersionExists,
  parseZeroVersion,
  readReleaseMode,
  readZeroPackageVersionAt,
  writeGithubOutput,
  zeroPackageName,
  zeroTag,
  type Exec,
  type CanaryZeroVersion,
  type HeadZeroVersion,
  type ReleaseBranch,
  type ReleaseMode,
  type ZeroTag,
  type ZeroVersion,
} from '../shared.ts';

const maintenanceBranchPattern = /^maint\/zero\/v\d+\.\d+$/;

export type ReleasePlan = {
  mode: ReleaseMode;
  release_branch: ReleaseBranch;
  version: ZeroVersion;
  tag: ZeroTag;
  source_sha: string;
  is_canary: string;
};

export type PlanReleaseOptions = {
  exec?: Exec | undefined;
  mode: string;
  releaseBranch: string;
  sourceSha?: string | undefined;
  workflowRefName: string;
};

export function runReleasePlanCli() {
  const plan = planRelease({
    mode: mustEnv('MODE'),
    releaseBranch: mustEnv('RELEASE_BRANCH'),
    sourceSha: process.env.SOURCE_SHA || undefined,
    workflowRefName: mustEnv('WORKFLOW_REF_NAME'),
  });

  writeGithubOutput(plan);
  console.log(`Release mode: ${plan.mode}`);
  console.log(`Release branch: ${plan.release_branch}`);
  console.log(`Source SHA: ${plan.source_sha}`);
  console.log(`Planned release version: ${plan.version}`);
  console.log(`Planned release tag: ${plan.tag}`);
}

export function planRelease({
  exec = defaultExec,
  mode: modeArg,
  releaseBranch,
  sourceSha: sourceShaOverride,
  workflowRefName,
}: PlanReleaseOptions): ReleasePlan {
  const mode = readReleaseMode(modeArg);

  assertMainWorkflowRef('Release', workflowRefName);
  if (!isAllowedReleaseBranch(releaseBranch)) {
    throw new Error(
      `Unsupported release branch ${releaseBranch}. Expected main or maint/zero/vX.Y`,
    );
  }
  if (mode === 'head' && releaseBranch !== 'main') {
    throw new Error(
      `Head releases are only supported from main, got ${releaseBranch}`,
    );
  }

  exec(
    'git',
    [
      'fetch',
      'origin',
      `+refs/heads/${releaseBranch}:refs/remotes/origin/${releaseBranch}`,
      '--tags',
    ],
    {stdio: 'inherit'},
  );

  let sourceSha: string;
  if (sourceShaOverride) {
    assertGitSha(sourceShaOverride, 'source SHA');
    sourceSha = sourceShaOverride;
  } else {
    sourceSha = exec('git', [
      'rev-parse',
      `refs/remotes/origin/${releaseBranch}`,
    ]).trim();
    assertGitSha(sourceSha, 'source SHA');
  }

  const currentVersion = readZeroPackageVersionAt(sourceSha, exec);
  const version =
    mode === 'stable'
      ? planStableVersion(currentVersion)
      : mode === 'head'
        ? planHeadVersion(currentVersion, sourceSha)
        : planCanaryVersion(
            currentVersion,
            readCanaryTags(exec, currentVersion),
          );
  const tag = zeroTag(version);

  if (gitTagExists(tag, exec)) {
    throw new Error(`Git tag ${tag} already exists`);
  }
  if (npmZeroVersionExists(version, exec)) {
    throw new Error(`${zeroPackageName}@${version} already exists on npm`);
  }

  return {
    mode,
    release_branch: releaseBranch,
    version,
    tag,
    source_sha: sourceSha,
    is_canary: String(mode === 'canary'),
  };
}

export function isAllowedReleaseBranch(
  refName: string,
): refName is ReleaseBranch {
  return refName === 'main' || maintenanceBranchPattern.test(refName);
}

export function planStableVersion(currentVersion: string) {
  assertStableZeroVersion(
    currentVersion,
    `Stable releases require a stable package version, got ${currentVersion}`,
  );
  return currentVersion;
}

export function planCanaryVersion(
  currentVersion: string,
  existingTags: readonly string[],
): CanaryZeroVersion {
  const parsed = parseZeroVersion(currentVersion);
  if (!parsed) {
    throw new Error(
      `Cannot plan canary from package version ${currentVersion}. Expected X.Y.Z or X.Y.Z-canary.N`,
    );
  }

  const tagPattern = new RegExp(
    `^zero/v${escapeRegExp(parsed.baseVersion)}-canary\\.(\\d+)$`,
  );
  let maxAttempt = -1;
  for (const tag of existingTags) {
    const tagMatch = tag.match(tagPattern);
    if (!tagMatch) {
      continue;
    }
    maxAttempt = Math.max(maxAttempt, Number(tagMatch[1]));
  }

  return `${parsed.baseVersion}-canary.${maxAttempt + 1}` as CanaryZeroVersion;
}

export function planHeadVersion(
  currentVersion: string,
  sourceSha: string,
  now = new Date(),
): HeadZeroVersion {
  const parsed = parseZeroVersion(currentVersion);
  if (!parsed) {
    throw new Error(
      `Cannot plan head from package version ${currentVersion}. Expected X.Y.Z, X.Y.Z-canary.N, or X.Y.Z-head-SHA-DATE`,
    );
  }
  assertGitSha(sourceSha, 'source SHA');
  // The source commit rides in the version itself, so provenance needs
  // nothing written beyond the version; the UTC date is for humans. A
  // re-release of the same commit on the same day plans the same version
  // and is rejected by the caller's npm-exists check.
  const shaPrefix = sourceSha.slice(0, headVersionShaLength);
  const date = now.toISOString().slice(0, 10).replaceAll('-', '');
  return `${parsed.baseVersion}-head-${shaPrefix}-${date}` as HeadZeroVersion;
}

function readCanaryTags(exec: Exec, currentVersion: string) {
  const parsed = parseZeroVersion(currentVersion);
  if (!parsed) {
    return [];
  }
  const tagPrefix = `zero/v${parsed.baseVersion}-canary.`;
  return exec('git', ['tag', '--list', `${tagPrefix}*`])
    .split('\n')
    .filter(Boolean);
}
