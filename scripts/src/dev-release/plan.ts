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
const dockerTagPattern = /^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$/;
const protectedTags = new Set(['latest', 'head', 'staging', 'canary']);

export type DevReleasePlan = {
  image_tag: string;
  ref: string;
  source_sha: string;
};

export type PlanDevReleaseOptions = {
  exec?: Exec | undefined;
  imageTagInput?: string | undefined;
  targetRef: string;
  workflowRefName: string;
};

export function runDevReleasePlanCli() {
  const plan = planDevRelease({
    imageTagInput: process.env.IMAGE_TAG_INPUT || undefined,
    targetRef: mustEnv('TARGET_REF'),
    workflowRefName: mustEnv('WORKFLOW_REF_NAME'),
  });

  writeGithubOutput(plan);
  console.log(`Target ref: ${plan.ref}`);
  console.log(`Source SHA: ${plan.source_sha}`);
  console.log(`Planned image tag: ${plan.image_tag}`);
}

export function planDevRelease({
  exec = defaultExec,
  imageTagInput,
  targetRef,
  workflowRefName,
}: PlanDevReleaseOptions): DevReleasePlan {
  assertMainWorkflowRef('Dev release', workflowRefName);

  if (!targetRef.trim()) {
    throw new Error('Target ref must not be empty');
  }

  const rawInput = imageTagInput?.trim();
  if (rawInput && protectedTags.has(rawInput.toLowerCase())) {
    throw new Error(
      `Tag "${rawInput}" is protected and cannot be overwritten by dev releases.`,
    );
  }

  const sourceSha = resolveSourceSha(targetRef.trim(), exec);
  assertGitSha(sourceSha, 'source SHA');

  const imageTag = rawInput
    ? normalizeDevImageTag(rawInput)
    : deriveDefaultTag(targetRef.trim(), sourceSha);

  validateImageTag(imageTag);

  return {
    image_tag: imageTag,
    ref: targetRef.trim(),
    source_sha: sourceSha,
  };
}

const gitRefPrefixPattern = /^refs\/(heads|remotes\/origin|remotes|pull)\//;
const invalidSemVerPrereleaseCharPattern = /[^a-zA-Z0-9-]/g;
const consecutiveHyphenPattern = /-+/g;
const edgeHyphenPattern = /^-+|-+$/g;

export function sanitizeBranchName(branch: string): string {
  return branch
    .replace(gitRefPrefixPattern, '')
    .replace(invalidSemVerPrereleaseCharPattern, '-')
    .replace(consecutiveHyphenPattern, '-')
    .replace(edgeHyphenPattern, '');
}

export function deriveDefaultTag(targetRef: string, sourceSha: string): string {
  const shortSha = sourceSha.slice(0, 8);
  if (gitShaPattern.test(targetRef)) {
    return `0.0.0-dev-${shortSha}`;
  }
  const rawClean = targetRef.replace(gitRefPrefixPattern, '');
  if (rawClean.startsWith('0.0.0-')) {
    const prerelease = sanitizeBranchName(rawClean.slice('0.0.0-'.length));
    return `0.0.0-${prerelease}`.slice(0, 128);
  }
  const clean = sanitizeBranchName(targetRef);
  if (!clean) {
    return `0.0.0-dev-${shortSha}`;
  }
  if (clean.startsWith('pr-') || clean.startsWith('dev-')) {
    return `0.0.0-${clean}`.slice(0, 128);
  }
  return `0.0.0-pr-${clean}`.slice(0, 128);
}

export function normalizeDevImageTag(input: string): string {
  const trimmed = input.trim();
  if (trimmed.startsWith('0.0.0-')) {
    return trimmed;
  }
  if (trimmed.startsWith('dev-') || trimmed.startsWith('pr-')) {
    return `0.0.0-${trimmed}`;
  }
  return `0.0.0-dev-${trimmed}`;
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

export function resolveSourceSha(targetRef: string, exec: Exec): string {
  if (targetRef.startsWith('-')) {
    throw new Error('Target ref must not start with "-"');
  }

  if (gitShaPattern.test(targetRef)) {
    try {
      return exec('git', [
        'rev-parse',
        '--verify',
        `${targetRef}^{commit}`,
      ]).trim();
    } catch {
      // If the commit is not present locally, attempt to fetch it.
      exec('git', ['fetch', 'origin', targetRef], {stdio: 'inherit'});
      return exec('git', [
        'rev-parse',
        '--verify',
        'FETCH_HEAD^{commit}',
      ]).trim();
    }
  }

  // Target ref is a branch, tag, or PR ref (e.g. refs/pull/123/head).
  exec('git', ['fetch', 'origin', targetRef], {stdio: 'inherit'});
  return exec('git', ['rev-parse', '--verify', 'FETCH_HEAD^{commit}']).trim();
}
