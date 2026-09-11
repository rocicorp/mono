// oxlint-disable no-console

import {
  assertGitSha,
  assertMainWorkflowRef,
  defaultExec,
  mustEnv,
  readZeroPackageVersionAt,
  writeGithubOutput,
  type Exec,
} from '../shared.ts';

const gitShaPattern = /^[0-9a-f]{40}$/;
const hexShaPattern = /^[0-9a-f]{7,40}$/i;
const dockerTagPattern = /^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$/;
const semverRegex =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/;
const semverPrefixPattern = /^(\d+)\.(\d+)\.(\d+)/;
const protectedTags = new Set(['latest', 'head', 'staging', 'canary']);

export type DevReleasePlan = {
  image_tag: string;
  ref: string;
  source_sha: string;
};

export type PlanDevReleaseOptions = {
  branchInput?: string | undefined;
  commitShaInput?: string | undefined;
  exec?: Exec | undefined;
  workflowRefName: string;
};

export function nextPatchVersion(version: string): string {
  const match = version.match(semverPrefixPattern);
  if (!match) {
    throw new Error(`Cannot derive next patch version from "${version}"`);
  }
  const major = match[1];
  const minor = match[2];
  const patch = Number(match[3]) + 1;
  return `${major}.${minor}.${patch}`;
}

export function runDevReleasePlanCli() {
  const plan = planDevRelease({
    branchInput: process.env.BRANCH_INPUT || 'main',
    commitShaInput: process.env.COMMIT_SHA_INPUT || undefined,
    workflowRefName: mustEnv('WORKFLOW_REF_NAME'),
  });

  writeGithubOutput(plan);
  console.log(`Target ref: ${plan.ref}`);
  console.log(`Source SHA: ${plan.source_sha}`);
  console.log(`Planned image tag: ${plan.image_tag}`);
}

export function planDevRelease({
  branchInput = 'main',
  commitShaInput,
  exec = defaultExec,
  workflowRefName,
}: PlanDevReleaseOptions): DevReleasePlan {
  assertMainWorkflowRef('Dev release', workflowRefName);

  const trimmedBranch = branchInput.trim();
  if (!trimmedBranch) {
    throw new Error('Branch must not be empty');
  }

  const trimmedCommitSha = commitShaInput?.trim() || undefined;

  const sourceSha = resolveSourceSha({
    branch: trimmedBranch,
    commitSha: trimmedCommitSha,
    exec,
  });
  assertGitSha(sourceSha, 'source SHA');

  const shortSha = resolveUniqueShortSha(sourceSha, exec);
  const sourceVersion = readZeroPackageVersionAt(sourceSha, exec);
  const nextPatch = nextPatchVersion(sourceVersion);
  const imageTag = deriveDevImageTag(
    trimmedBranch,
    sourceSha,
    nextPatch,
    shortSha,
  );
  validateImageTag(imageTag);

  return {
    image_tag: imageTag,
    ref: trimmedBranch,
    source_sha: sourceSha,
  };
}

const gitRefPrefixPattern = /^refs\/(heads|remotes\/origin|remotes)\//;
const prRefPattern = /(?:refs\/)?pull\/(\d+)(?:\/head)?/;
const invalidSemVerPrereleaseCharPattern = /[^a-zA-Z0-9-]/g;
const consecutiveHyphenPattern = /-+/g;
const edgeHyphenPattern = /^-+|-+$/g;
const trailingHyphenPattern = /-+$/;

export function sanitizeBranchName(branch: string): string {
  const prMatch = branch.match(prRefPattern);
  if (prMatch) {
    return `pr-${prMatch[1]}`;
  }
  return branch
    .replace(gitRefPrefixPattern, '')
    .replace(invalidSemVerPrereleaseCharPattern, '-')
    .replace(consecutiveHyphenPattern, '-')
    .replace(edgeHyphenPattern, '');
}

export function resolveUniqueShortSha(
  sourceSha: string,
  exec: Exec,
  minLen = 8,
): string {
  const shortSha = exec('git', [
    'rev-parse',
    `--short=${minLen}`,
    `${sourceSha}^{commit}`,
  ]).trim();
  if (!shortSha || !hexShaPattern.test(shortSha)) {
    throw new Error(
      `Failed to resolve short SHA for "${sourceSha}": git returned "${shortSha}"`,
    );
  }
  return shortSha;
}

export function deriveDevImageTag(
  branch: string,
  sourceSha: string,
  nextPatch = '0.0.1',
  shortSha = sourceSha.slice(0, 8),
): string {
  const prefix = `${nextPatch}-dev-`;
  if (gitShaPattern.test(branch.trim())) {
    return `${prefix}${shortSha}`;
  }

  const rawClean = branch.trim().replace(gitRefPrefixPattern, '');

  let base: string;
  if (rawClean.startsWith(prefix)) {
    base = sanitizeBranchName(rawClean.slice(prefix.length));
  } else if (rawClean.startsWith('0.0.0-')) {
    base = sanitizeBranchName(rawClean.slice('0.0.0-'.length));
  } else {
    base = sanitizeBranchName(rawClean);
  }

  if (base.startsWith('dev-')) {
    base = base.slice('dev-'.length);
  }

  if (base.endsWith(`-${shortSha}`)) {
    base = base.slice(0, -`-${shortSha}`.length);
  } else if (base.endsWith(`-${sourceSha.slice(0, 8)}`)) {
    base = base.slice(0, -`-${sourceSha.slice(0, 8)}`.length);
  }

  if (!base) {
    return `${prefix}${shortSha}`;
  }

  const maxBaseLen = 128 - prefix.length - 1 - shortSha.length;
  const trimmedBase = base
    .slice(0, maxBaseLen)
    .replace(trailingHyphenPattern, '');

  return `${prefix}${trimmedBase}-${shortSha}`;
}

export function validateImageTag(tag: string): void {
  if (!dockerTagPattern.test(tag)) {
    throw new Error(
      `Invalid Docker image tag "${tag}". Tag must contain only letters, numbers, underscores, periods, and hyphens.`,
    );
  }
  if (protectedTags.has(tag.toLowerCase())) {
    throw new Error(
      `Tag "${tag}" is protected and cannot be overwritten by dev releases.`,
    );
  }
  if (!semverRegex.test(tag)) {
    throw new Error(
      `Tag "${tag}" is not a valid semantic version (SemVer 2.0.0). Prerelease identifiers may only contain alphanumerics and hyphens.`,
    );
  }
  if (!tag.includes('-dev-')) {
    throw new Error(
      `Tag "${tag}" must be a dev prerelease version containing "-dev-" to prevent colliding with official releases.`,
    );
  }
}

export type ResolveSourceShaOptions = {
  branch: string;
  commitSha?: string | undefined;
  exec: Exec;
};

export function resolveSourceSha({
  branch,
  commitSha,
  exec,
}: ResolveSourceShaOptions): string {
  if (branch.startsWith('-')) {
    throw new Error('Branch must not start with "-"');
  }

  if (commitSha) {
    if (commitSha.startsWith('-')) {
      throw new Error('Commit SHA must not start with "-"');
    }
    if (!hexShaPattern.test(commitSha)) {
      throw new Error(`Invalid commit SHA "${commitSha}"`);
    }

    if (!gitShaPattern.test(branch)) {
      exec('git', ['fetch', 'origin', branch], {stdio: 'inherit'});
    }

    try {
      return exec('git', [
        'rev-parse',
        '--verify',
        `${commitSha}^{commit}`,
      ]).trim();
    } catch {
      exec('git', ['fetch', 'origin', commitSha], {stdio: 'inherit'});
      return exec('git', [
        'rev-parse',
        '--verify',
        'FETCH_HEAD^{commit}',
      ]).trim();
    }
  }

  if (gitShaPattern.test(branch)) {
    try {
      return exec('git', [
        'rev-parse',
        '--verify',
        `${branch}^{commit}`,
      ]).trim();
    } catch {
      exec('git', ['fetch', 'origin', branch], {stdio: 'inherit'});
      return exec('git', [
        'rev-parse',
        '--verify',
        'FETCH_HEAD^{commit}',
      ]).trim();
    }
  }

  exec('git', ['fetch', 'origin', branch], {stdio: 'inherit'});
  return exec('git', ['rev-parse', '--verify', 'FETCH_HEAD^{commit}']).trim();
}
