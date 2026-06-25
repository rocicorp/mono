#!/usr/bin/env node

import {execFileSync} from 'node:child_process';
import {existsSync, mkdirSync, readFileSync} from 'node:fs';
import {dirname, join} from 'node:path';

const DEFAULT_ALLOWED_SIGNERS_PATH = '.github/signing/allowed_signers';
const FULL_SHA_PATTERN = /^[0-9a-fA-F]{40}$/;
const INTEGER_PATTERN = /^[0-9]+$/;
const LINE_SPLIT_PATTERN = /\r?\n/;
const POLICY_REPOSITORY_PATTERN = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;
const POLICY_REF_PATTERN = /^[A-Za-z0-9._/-]+$/;
const POLICY_PATH_PATTERN = /^[A-Za-z0-9._/-]+$/;

try {
  main();
} catch (error) {
  workflowCommand(
    'error',
    error instanceof Error ? error.message : String(error),
  );
  process.exitCode = 1;
}

function main() {
  const workspace = process.env.GITHUB_WORKSPACE || process.cwd();
  const pr = readPullRequestPayload();
  const prNumber = validateInteger('pull request number', pr.number);
  const prCommitCount = validateInteger(
    'pull request commit count',
    pr.commits,
  );
  const prHeadSha = validateSha('pull request head SHA', pr.head?.sha);
  const prBaseSha = validateSha('pull request base SHA', pr.base?.sha);

  const policyRepository =
    process.env.SIGNED_COMMIT_POLICY_REPOSITORY ||
    `${requiredEnv('GITHUB_REPOSITORY_OWNER')}/security-policy`;
  validatePolicyRepository(policyRepository);

  const policyRef = process.env.SIGNED_COMMIT_POLICY_REF || 'main';
  validatePolicyRef(policyRef);

  const policyPath =
    process.env.SIGNED_COMMIT_ALLOWED_SIGNERS_PATH ||
    DEFAULT_ALLOWED_SIGNERS_PATH;
  validatePolicyPath(policyPath);

  const allowedSignersPath = join(
    process.env.RUNNER_TEMP || workspace,
    'signed-commit-authors',
    'allowed_signers',
  );

  downloadAllowedSigners({
    allowedSignersPath,
    policyPath,
    policyRef,
    policyRepository,
  });

  const activeSignerCount = countActiveSigners(allowedSignersPath);
  info(
    `Checking signatures against ${activeSignerCount} allowed SSH key entr${activeSignerCount === 1 ? 'y' : 'ies'}.`,
  );

  fetchPullRequestCommits({
    prCommitCount,
    prHeadSha,
    prNumber,
    workspace,
  });

  info(`Checking ${prCommitCount} commit(s) on PR #${prNumber}.`);
  verifyPullRequestCommits({
    allowedSignersPath,
    expectedCommitCount: prCommitCount,
    prBaseSha,
    prHeadSha,
    workspace,
  });
}

function readPullRequestPayload() {
  const eventPath = requiredEnv('GITHUB_EVENT_PATH');
  const payload = JSON.parse(readFileSync(eventPath, 'utf8'));
  if (!payload.pull_request) {
    fail('No pull_request payload.');
  }
  return payload.pull_request;
}

function downloadAllowedSigners({
  allowedSignersPath,
  policyPath,
  policyRef,
  policyRepository,
}) {
  const token = requiredEnv('SECURITY_POLICY_REPO_TOKEN');
  mkdirSync(dirname(allowedSignersPath), {recursive: true});

  const encodedPath = policyPath
    .split('/')
    .map(segment => encodeURIComponent(segment))
    .join('/');
  const url = new URL(
    `https://api.github.com/repos/${policyRepository}/contents/${encodedPath}`,
  );
  url.searchParams.set('ref', policyRef);

  const curl = execFile('curl', [
    '--fail-with-body',
    '--silent',
    '--show-error',
    '--location',
    '--header',
    'Accept: application/vnd.github.raw+json',
    '--header',
    `Authorization: Bearer ${token}`,
    '--header',
    'X-GitHub-Api-Version: 2022-11-28',
    url.toString(),
    '--output',
    allowedSignersPath,
  ]);

  if (!curl.ok) {
    fail(
      `Could not download signing policy from ${policyRepository}:${policyPath}@${policyRef}: ${commandDetails(curl)}`,
    );
  }
  if (!existsSync(allowedSignersPath)) {
    fail(
      `Signing policy was not downloaded from ${policyRepository}:${policyPath}@${policyRef}.`,
    );
  }
  if (readFileSync(allowedSignersPath, 'utf8').length === 0) {
    fail(
      `Downloaded signing policy is empty: ${policyRepository}:${policyPath}@${policyRef}.`,
    );
  }
}

function countActiveSigners(allowedSignersPath) {
  const activeSignerLines = readFileSync(allowedSignersPath, 'utf8')
    .split(LINE_SPLIT_PATTERN)
    .map(line => line.trim())
    .filter(line => line && !line.startsWith('#'));

  if (!activeSignerLines.length) {
    fail(`${allowedSignersPath} has no active signing keys.`);
  }
  return activeSignerLines.length;
}

function fetchPullRequestCommits({
  prCommitCount,
  prHeadSha,
  prNumber,
  workspace,
}) {
  const token = requiredEnv('GITHUB_TOKEN');
  const authHeader = Buffer.from(`x-access-token:${token}`, 'utf8').toString(
    'base64',
  );

  const fetch = git(
    [
      '-c',
      `http.https://github.com/.extraheader=AUTHORIZATION: basic ${authHeader}`,
      'fetch',
      '--no-tags',
      `--depth=${prCommitCount + 1}`,
      'origin',
      `+refs/pull/${prNumber}/head:refs/remotes/pull/${prNumber}/head`,
    ],
    workspace,
  );
  if (!fetch.ok) {
    fail(`Could not fetch PR commits: ${commandDetails(fetch)}`);
  }

  const headExists = git(
    ['cat-file', '-e', `${prHeadSha}^{commit}`],
    workspace,
  );
  if (!headExists.ok) {
    fail(`Could not find fetched PR head commit ${prHeadSha}.`);
  }
}

function verifyPullRequestCommits({
  allowedSignersPath,
  expectedCommitCount,
  prBaseSha,
  prHeadSha,
  workspace,
}) {
  const revList = git(
    ['rev-list', '--reverse', `${prBaseSha}..${prHeadSha}`],
    workspace,
  );
  if (!revList.ok) {
    fail(`Could not list PR commits locally: ${commandDetails(revList)}`);
  }

  const commits = revList.stdout
    .trim()
    .split(LINE_SPLIT_PATTERN)
    .filter(Boolean);
  if (commits.length !== expectedCommitCount) {
    fail(
      `Expected ${expectedCommitCount} PR commit(s) from the pull_request payload, but git rev-list found ${commits.length}.`,
    );
  }

  const failures = [];
  for (const commit of commits) {
    const shortSha = commit.slice(0, 12);
    const subject = commitSubject(commit, workspace);
    const signatureCheck = verifyAllowedSignature(
      commit,
      allowedSignersPath,
      workspace,
    );

    if (!signatureCheck.allowed) {
      failures.push({
        problem: `signature is not made by an allowed SSH signing key: ${signatureCheck.detail}`,
        sha: shortSha,
        subject,
      });
      continue;
    }

    info(
      `${shortSha}: allowed SSH signature for ${signatureCheck.principal || '(unnamed principal)'} using ${signatureCheck.fingerprint}`,
    );
  }

  if (failures.length) {
    const details = failures
      .map(f => `- ${f.sha}: ${f.problem} - ${f.subject}`)
      .join('\n');
    fail(
      `Signed commit author check failed for ${failures.length}/${commits.length} commit(s):\n${details}`,
    );
  }

  workflowCommand(
    'notice',
    `All ${commits.length} PR commit(s) are signed by allowed SSH keys.`,
  );
}

function verifyAllowedSignature(commit, allowedSignersPath, workspace) {
  const verify = signingGit(
    ['verify-commit', commit],
    allowedSignersPath,
    workspace,
  );
  if (!verify.ok) {
    return {
      allowed: false,
      detail:
        commandDetails(verify) ||
        'signature is missing or not made by an allowed SSH key',
    };
  }

  const metadata = signingGit(
    ['show', '-s', '--format=%GS%x00%GK%x00%GT', commit],
    allowedSignersPath,
    workspace,
  );
  if (!metadata.ok) {
    return {
      allowed: false,
      detail: commandDetails(metadata) || 'could not read signature metadata',
    };
  }

  const [principal = '', fingerprint = '', trust = ''] = metadata.stdout
    .trimEnd()
    .split('\0');
  if (trust !== 'fully') {
    return {
      allowed: false,
      detail: `signature trust is ${trust || 'unknown'}, not fully trusted`,
    };
  }
  if (!fingerprint) {
    return {
      allowed: false,
      detail: 'signature did not report a key fingerprint',
    };
  }

  return {
    allowed: true,
    fingerprint,
    principal,
  };
}

function commitSubject(commit, workspace) {
  const subject = git(['show', '-s', '--format=%s', commit], workspace);
  if (subject.ok) {
    return subject.stdout.trim();
  }
  return '(could not read commit subject)';
}

function signingGit(args, allowedSignersPath, workspace) {
  return git(
    [
      '-c',
      `gpg.ssh.allowedSignersFile=${allowedSignersPath}`,
      '-c',
      'gpg.minTrustLevel=fully',
      ...args,
    ],
    workspace,
  );
}

function git(args, workspace) {
  return execFile('git', args, {cwd: workspace});
}

function execFile(command, args, options = {}) {
  try {
    return {
      ok: true,
      stderr: '',
      stdout: execFileSync(command, args, {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
        ...options,
      }),
    };
  } catch (error) {
    return {
      ok: false,
      stderr: String(error.stderr ?? ''),
      stdout: String(error.stdout ?? ''),
    };
  }
}

function commandDetails(result) {
  return `${result.stdout}\n${result.stderr}`.trim();
}

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    fail(`Missing ${name}.`);
  }
  return value;
}

function validateInteger(label, value) {
  const text = String(value);
  if (!INTEGER_PATTERN.test(text)) {
    fail(`Invalid ${label}: ${text}`);
  }
  return Number.parseInt(text, 10);
}

function validateSha(label, value) {
  const text = String(value);
  if (!FULL_SHA_PATTERN.test(text)) {
    fail(`Invalid ${label}: ${text}`);
  }
  return text;
}

function validatePolicyRepository(repository) {
  if (!POLICY_REPOSITORY_PATTERN.test(repository)) {
    fail(`Invalid signing policy repository: ${repository}`);
  }
}

function validatePolicyRef(ref) {
  if (
    !ref ||
    ref.includes('..') ||
    ref.startsWith('/') ||
    !POLICY_REF_PATTERN.test(ref)
  ) {
    fail(`Invalid signing policy ref: ${ref}`);
  }
}

function validatePolicyPath(path) {
  if (
    !path ||
    path.startsWith('/') ||
    path.split('/').some(segment => segment === '' || segment === '..') ||
    !POLICY_PATH_PATTERN.test(path)
  ) {
    fail(`Invalid signing policy path: ${path}`);
  }
}

function info(message) {
  process.stdout.write(`${message}\n`);
}

function fail(message) {
  throw new Error(message);
}

function workflowCommand(command, message) {
  process.stdout.write(`::${command}::${escapeWorkflowCommand(message)}\n`);
}

function escapeWorkflowCommand(message) {
  return String(message)
    .replace(/%/g, '%25')
    .replace(/\r/g, '%0D')
    .replace(/\n/g, '%0A');
}
