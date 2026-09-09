// oxlint-disable no-console

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
const semverPattern = /^v?\d+\.\d+\.\d+$/;
const protectedTags = new Set(['latest', 'head', 'staging', 'canary']);

export type DevReleasePlan = {
  image_tag: string;
  ref: string;
  source_sha: string;
};

export type PlanDevReleaseOptions = {
  exec?: Exec | undefined;
  fetchFn?: typeof fetch | undefined;
  githubRepository?: string | undefined;
  githubToken?: string | undefined;
  imageTagInput?: string | undefined;
  requireCommitVerification?: boolean | undefined;
  targetRef: string;
  workflowRefName: string;
};

export async function runDevReleasePlanCli() {
  const plan = await planDevRelease({
    githubRepository: process.env.GITHUB_REPOSITORY,
    githubToken: process.env.GITHUB_TOKEN,
    imageTagInput: process.env.IMAGE_TAG_INPUT || undefined,
    targetRef: mustEnv('TARGET_REF'),
    workflowRefName: mustEnv('WORKFLOW_REF_NAME'),
  });

  writeGithubOutput(plan);
  console.log(`Target ref: ${plan.ref}`);
  console.log(`Source SHA: ${plan.source_sha}`);
  console.log(`Planned image tag: ${plan.image_tag}`);
}

export async function planDevRelease({
  exec = defaultExec,
  fetchFn = fetch,
  githubRepository,
  githubToken,
  imageTagInput,
  requireCommitVerification = true,
  targetRef,
  workflowRefName,
}: PlanDevReleaseOptions): Promise<DevReleasePlan> {
  assertMainWorkflowRef('Dev release', workflowRefName);

  if (!targetRef.trim()) {
    throw new Error('Target ref must not be empty');
  }

  const sourceSha = resolveSourceSha(targetRef.trim(), exec);
  assertGitSha(sourceSha, 'source SHA');

  const imageTag = imageTagInput?.trim()
    ? imageTagInput.trim()
    : deriveDefaultTag(targetRef.trim(), sourceSha);

  validateImageTag(imageTag);

  if (requireCommitVerification && githubToken && githubRepository) {
    await verifyCommit({
      fetchFn,
      githubRepository,
      githubToken,
      sourceSha,
    });
  }

  return {
    image_tag: imageTag,
    ref: targetRef.trim(),
    source_sha: sourceSha,
  };
}

const gitRefPrefixPattern = /^refs\/(heads|remotes\/origin|remotes|pull)\//;
const invalidTagCharPattern = /[^a-zA-Z0-9_.-]/g;
const consecutiveHyphenPattern = /-+/g;
const edgeHyphenPattern = /^-+|-+$/g;

export function sanitizeBranchName(branch: string): string {
  return branch
    .replace(gitRefPrefixPattern, '')
    .replace(invalidTagCharPattern, '-')
    .replace(consecutiveHyphenPattern, '-')
    .replace(edgeHyphenPattern, '');
}

export function deriveDefaultTag(targetRef: string, sourceSha: string): string {
  if (gitShaPattern.test(targetRef)) {
    return `dev-${sourceSha.slice(0, 8)}`;
  }
  const clean = sanitizeBranchName(targetRef);
  if (!clean) {
    return `dev-${sourceSha.slice(0, 8)}`;
  }
  if (clean.startsWith('pr-') || clean.startsWith('dev-')) {
    return clean.slice(0, 128);
  }
  return `pr-${clean}`.slice(0, 128);
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
  if (semverPattern.test(tag)) {
    throw new Error(
      `Tag "${tag}" looks like a semantic version release. Dev releases must not use version numbers.`,
    );
  }
}

export function resolveSourceSha(targetRef: string, exec: Exec): string {
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

export async function verifyCommit({
  fetchFn,
  githubRepository,
  githubToken,
  sourceSha,
}: {
  fetchFn: typeof fetch;
  githubRepository: string;
  githubToken: string;
  sourceSha: string;
}): Promise<void> {
  const headers = {
    'Accept': 'application/vnd.github+json',
    'Authorization': `Bearer ${githubToken}`,
    'User-Agent': 'rocicorp-dev-release',
  };

  const checksUrl = `https://api.github.com/repos/${githubRepository}/commits/${sourceSha}/check-runs?per_page=100`;
  const checksRes = await fetchFn(checksUrl, {headers});
  if (!checksRes.ok) {
    throw new Error(
      `Failed to fetch check runs for ${sourceSha}: ${checksRes.status} ${checksRes.statusText}`,
    );
  }

  const checksData = (await checksRes.json()) as {
    check_runs?:
      | Array<{
          conclusion?: string | null | undefined;
          name?: string | undefined;
        }>
      | undefined;
  };

  const signedCommitCheck = checksData.check_runs?.find(
    c => c.name?.trim().toLowerCase() === 'signed commit authors',
  );

  if (signedCommitCheck?.conclusion === 'success') {
    console.log(`Commit ${sourceSha} passed "${signedCommitCheck.name}" check`);
    return;
  }

  const status = signedCommitCheck
    ? `conclusion was "${signedCommitCheck.conclusion}"`
    : 'check run was not found';

  throw new Error(
    `Commit ${sourceSha} has not passed the "Signed Commit Authors" check (${status}). Dev releases require the "Signed Commit Authors" check to pass.`,
  );
}
