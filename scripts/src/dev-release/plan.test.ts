import {expect, test} from 'vitest';
import {type Command, type Exec, type ExecOptions} from '../shared.ts';
import {
  deriveDefaultTag,
  planDevRelease,
  sanitizeBranchName,
  validateImageTag,
} from './plan.ts';

const dummySha = 'e8cc6889fa6bc2a364e8cb80776991c308601212';

function makeMockExec(resolvedSha = dummySha) {
  const calls: Array<{
    command: Command;
    args: readonly string[];
    options?: ExecOptions;
  }> = [];
  const exec: Exec = (command, args, options) => {
    calls.push({command, args, options});
    if (command === 'git' && args[0] === 'rev-parse') {
      return `${resolvedSha}\n`;
    }
    return '';
  };
  return {calls, exec};
}

test('sanitizeBranchName strips refs prefix and sanitizes special characters', () => {
  expect(sanitizeBranchName('refs/heads/greg/sync-opt')).toBe('greg-sync-opt');
  expect(sanitizeBranchName('refs/pull/123/head')).toBe('123-head');
  expect(sanitizeBranchName('feat/my_cool.branch!')).toBe(
    'feat-my_cool.branch',
  );
  expect(sanitizeBranchName('---messy--branch---')).toBe('messy-branch');
});

test('deriveDefaultTag generates pr- or dev- prefixed tags', () => {
  expect(deriveDefaultTag(dummySha, dummySha)).toBe('dev-e8cc6889');
  expect(deriveDefaultTag('greg/sync-opt', dummySha)).toBe('pr-greg-sync-opt');
  expect(deriveDefaultTag('pr-1234', dummySha)).toBe('pr-1234');
  expect(deriveDefaultTag('dev-test', dummySha)).toBe('dev-test');
});

test('validateImageTag accepts valid tags and blocks protected/semver tags', () => {
  expect(() => validateImageTag('pr-1234')).not.toThrow();
  expect(() => validateImageTag('bench_view-syncer.v1')).not.toThrow();

  expect(() => validateImageTag('latest')).toThrowError(/protected/);
  expect(() => validateImageTag('HEAD')).toThrowError(/protected/);
  expect(() => validateImageTag('staging')).toThrowError(/protected/);
  expect(() => validateImageTag('canary')).toThrowError(/protected/);

  expect(() => validateImageTag('1.8.0')).toThrowError(/semantic version/);
  expect(() => validateImageTag('v1.8.0')).toThrowError(/semantic version/);
  expect(() => validateImageTag('v0.18.0')).toThrowError(/semantic version/);

  expect(() => validateImageTag('invalid:tag')).toThrowError(
    /Invalid Docker image tag/,
  );
  expect(() => validateImageTag('invalid/tag')).toThrowError(
    /Invalid Docker image tag/,
  );
});

test('planDevRelease requires workflowRefName to be main', async () => {
  const {exec} = makeMockExec();
  await expect(
    planDevRelease({
      exec,
      targetRef: 'greg/test',
      workflowRefName: 'feature-branch',
      requireCommitVerification: false,
    }),
  ).rejects.toThrow(/must be run from main/);
});

test('planDevRelease rejects empty target ref', async () => {
  const {exec} = makeMockExec();
  await expect(
    planDevRelease({
      exec,
      targetRef: '   ',
      workflowRefName: 'main',
      requireCommitVerification: false,
    }),
  ).rejects.toThrow(/Target ref must not be empty/);
});

test('planDevRelease plans dev release with default tag', async () => {
  const {calls, exec} = makeMockExec();
  const plan = await planDevRelease({
    exec,
    targetRef: 'greg/sync-opt',
    workflowRefName: 'main',
    requireCommitVerification: false,
  });

  expect(plan).toEqual({
    image_tag: 'pr-greg-sync-opt',
    ref: 'greg/sync-opt',
    source_sha: dummySha,
  });

  expect(calls).toContainEqual({
    command: 'git',
    args: ['fetch', 'origin', 'greg/sync-opt'],
    options: {stdio: 'inherit'},
  });
});

test('planDevRelease accepts custom image tag', async () => {
  const {exec} = makeMockExec();
  const plan = await planDevRelease({
    exec,
    targetRef: dummySha,
    imageTagInput: 'custom-bench-1',
    workflowRefName: 'main',
    requireCommitVerification: false,
  });

  expect(plan).toEqual({
    image_tag: 'custom-bench-1',
    ref: dummySha,
    source_sha: dummySha,
  });
});

test('verifyCommit passes with Signed Commit Authors check run', async () => {
  const {exec} = makeMockExec();
  const mockFetch: typeof fetch = url => {
    const urlStr = String(url);
    if (urlStr.includes('/check-runs')) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            check_runs: [
              {name: 'Signed Commit Authors', conclusion: 'success'},
            ],
          }),
          {status: 200},
        ),
      );
    }
    return Promise.resolve(new Response('Not found', {status: 404}));
  };

  const plan = await planDevRelease({
    exec,
    fetchFn: mockFetch,
    githubRepository: 'rocicorp/mono',
    githubToken: 'dummy-token',
    requireCommitVerification: true,
    targetRef: 'main',
    workflowRefName: 'main',
  });

  expect(plan.source_sha).toBe(dummySha);
});

test('verifyCommit rejects commit when Signed Commit Authors check failed', async () => {
  const {exec} = makeMockExec();
  const mockFetch: typeof fetch = url => {
    const urlStr = String(url);
    if (urlStr.includes('/check-runs')) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            check_runs: [
              {name: 'Signed Commit Authors', conclusion: 'failure'},
            ],
          }),
          {status: 200},
        ),
      );
    }
    return Promise.resolve(new Response('Not found', {status: 404}));
  };

  await expect(
    planDevRelease({
      exec,
      fetchFn: mockFetch,
      githubRepository: 'rocicorp/mono',
      githubToken: 'dummy-token',
      requireCommitVerification: true,
      targetRef: 'main',
      workflowRefName: 'main',
    }),
  ).rejects.toThrow(
    /Commit .* has not passed the "Signed Commit Authors" check \(conclusion was "failure"\)/,
  );
});

test('verifyCommit rejects commit when Signed Commit Authors check is missing', async () => {
  const {exec} = makeMockExec();
  const mockFetch: typeof fetch = url => {
    const urlStr = String(url);
    if (urlStr.includes('/check-runs')) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            check_runs: [{name: 'Other Check', conclusion: 'success'}],
          }),
          {status: 200},
        ),
      );
    }
    return Promise.resolve(new Response('Not found', {status: 404}));
  };

  await expect(
    planDevRelease({
      exec,
      fetchFn: mockFetch,
      githubRepository: 'rocicorp/mono',
      githubToken: 'dummy-token',
      requireCommitVerification: true,
      targetRef: 'main',
      workflowRefName: 'main',
    }),
  ).rejects.toThrow(
    /Commit .* has not passed the "Signed Commit Authors" check \(check run was not found\)/,
  );
});
