// oxlint-disable no-console

import semver from 'semver';
import {
  assertGitSha,
  assertMainWorkflowRef,
  defaultExec,
  mustEnv,
  writeGithubOutput,
  type Exec,
} from '../shared.ts';

const gitShaPattern = /^[0-9a-f]{40}$/;
const hexShaPattern = /^[0-9a-f]{7,40}$/i;
const dockerTagPattern = /^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$/;
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
  const imageTag = deriveDevImageTag(trimmedBranch, sourceSha, shortSha);
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
  try {
    const shortSha = exec('git', [
      'rev-parse',
      `--short=${minLen}`,
      `${sourceSha}^{commit}`,
    ]).trim();
    if (shortSha && hexShaPattern.test(shortSha)) {
      return shortSha;
    }
  } catch {
    // Fallback if git rev-parse fails for any reason
  }
  return sourceSha.slice(0, minLen);
}

export function deriveDevImageTag(
  branch: string,
  sourceSha: string,
  shortSha = sourceSha.slice(0, 8),
): string {
  if (gitShaPattern.test(branch.trim())) {
    return `0.0.0-dev-${shortSha}`;
  }

  const rawClean = branch.trim().replace(gitRefPrefixPattern, '');

  let base: string;
  if (rawClean.startsWith('0.0.0-')) {
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
    return `0.0.0-dev-${shortSha}`;
  }

  const maxBaseLen = 128 - '0.0.0-dev-'.length - 1 - shortSha.length;
  const trimmedBase = base
    .slice(0, maxBaseLen)
    .replace(trailingHyphenPattern, '');

  return `0.0.0-dev-${trimmedBase}-${shortSha}`;
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
  if (!tag.startsWith('0.0.0-')) {
    throw new Error(
      `Tag "${tag}" must start with "0.0.0-" to ensure CloudZero SemVer compatibility and prevent colliding with official releases.`,
    );
  }
  if (!semver.valid(tag)) {
    throw new Error(
      `Tag "${tag}" is not a valid semantic version (SemVer 2.0.0). Prerelease identifiers may only contain alphanumerics and hyphens.`,
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
