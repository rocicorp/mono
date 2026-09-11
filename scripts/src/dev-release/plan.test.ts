import {expect, test} from 'vitest';
import {type Command, type Exec, type ExecOptions} from '../shared.ts';
import {
  deriveDevImageTag,
  nextPatchVersion,
  planDevRelease,
  resolveSourceSha,
  resolveUniqueShortSha,
  sanitizeBranchName,
  validateImageTag,
} from './plan.ts';

const dummySha = 'e8cc6889fa6bc2a364e8cb80776991c308601212';

function makeMockExec(
  resolvedSha = dummySha,
  shortSha?: string,
  packageVersion = '1.11.0',
) {
  const calls: Array<{
    command: Command;
    args: readonly string[];
    options?: ExecOptions | undefined;
  }> = [];
  const exec: Exec = (command, args, options) => {
    calls.push({command, args, options});
    if (command === 'git' && args[0] === 'rev-parse') {
      if (args[1]?.startsWith('--short')) {
        return `${shortSha ?? resolvedSha.slice(0, 8)}\n`;
      }
      return `${resolvedSha}\n`;
    }
    if (
      command === 'git' &&
      args[0] === 'show' &&
      args[1]?.endsWith(':packages/zero/package.json')
    ) {
      return `${JSON.stringify({version: packageVersion})}\n`;
    }
    return '';
  };
  return {calls, exec};
}

test('nextPatchVersion increments patch component of version string', () => {
  expect(nextPatchVersion('1.11.0')).toBe('1.11.1');
  expect(nextPatchVersion('0.25.1')).toBe('0.25.2');
  expect(nextPatchVersion('0.24.0')).toBe('0.24.1');
  expect(nextPatchVersion('0.0.0')).toBe('0.0.1');
  expect(nextPatchVersion('1.2.3-canary.1')).toBe('1.2.4');
  expect(() => nextPatchVersion('invalid')).toThrow(
    /Cannot derive next patch version/,
  );
});

test('sanitizeBranchName strips refs prefix, formats PR refs, and sanitizes special characters to hyphens', () => {
  expect(sanitizeBranchName('main')).toBe('main');
  expect(sanitizeBranchName('refs/heads/greg/sync-opt')).toBe('greg-sync-opt');
  expect(sanitizeBranchName('refs/pull/123/head')).toBe('pr-123');
  expect(sanitizeBranchName('pull/456')).toBe('pr-456');
  expect(sanitizeBranchName('feat/my_cool.branch!')).toBe(
    'feat-my-cool-branch',
  );
  expect(sanitizeBranchName('---messy--branch---')).toBe('messy-branch');
});

test('deriveDevImageTag generates <nextPatch>-dev-<branch>-<shortSha> SemVer tags', () => {
  expect(deriveDevImageTag('main', dummySha, '1.11.1')).toBe(
    '1.11.1-dev-main-e8cc6889',
  );
  expect(deriveDevImageTag('greg/sync-opt', dummySha, '1.11.1')).toBe(
    '1.11.1-dev-greg-sync-opt-e8cc6889',
  );
  expect(deriveDevImageTag('refs/pull/123/head', dummySha, '1.11.1')).toBe(
    '1.11.1-dev-pr-123-e8cc6889',
  );
  expect(deriveDevImageTag(dummySha, dummySha, '1.11.1')).toBe(
    '1.11.1-dev-e8cc6889',
  );
  expect(deriveDevImageTag('dev-benchmark', dummySha, '1.11.1')).toBe(
    '1.11.1-dev-benchmark-e8cc6889',
  );
  expect(deriveDevImageTag('sync-opt-e8cc6889', dummySha, '1.11.1')).toBe(
    '1.11.1-dev-sync-opt-e8cc6889',
  );
});

test('deriveDevImageTag truncates excessively long branch names while preserving the short SHA suffix within 128 chars', () => {
  const longBranch = 'a'.repeat(150);
  const tag = deriveDevImageTag(longBranch, dummySha, '1.11.1');
  expect(tag.length).toBeLessThanOrEqual(128);
  expect(tag.endsWith('-e8cc6889')).toBe(true);
  expect(tag.startsWith('1.11.1-dev-')).toBe(true);
  expect(() => validateImageTag(tag)).not.toThrow();
});

test('validateImageTag accepts valid SemVer dev tags and blocks invalid/protected/stable tags', () => {
  expect(() => validateImageTag('1.11.1-dev-main-e8cc6889')).not.toThrow();
  expect(() =>
    validateImageTag('1.11.1-dev-greg-sync-opt-e8cc6889'),
  ).not.toThrow();
  expect(() => validateImageTag('1.11.1-dev-e8cc6889')).not.toThrow();
  expect(() => validateImageTag('0.0.0-dev-main-e8cc6889')).not.toThrow();

  expect(() => validateImageTag('latest')).toThrowError(/protected/);
  expect(() => validateImageTag('HEAD')).toThrowError(/protected/);
  expect(() => validateImageTag('staging')).toThrowError(/protected/);
  expect(() => validateImageTag('canary')).toThrowError(/protected/);

  expect(() => validateImageTag('1.8.0')).toThrowError(
    /must be a dev prerelease version/,
  );
  expect(() => validateImageTag('1.8.0-canary.1')).toThrowError(
    /must be a dev prerelease version/,
  );
  expect(() => validateImageTag('1.8.0-head-e8cc6889-20260708')).toThrowError(
    /must be a dev prerelease version/,
  );
  expect(() => validateImageTag('1.8.0-rc.1')).toThrowError(
    /must be a dev prerelease version/,
  );
  expect(() => validateImageTag('v1.8.0')).toThrowError(
    /not a valid semantic version/,
  );
  expect(() => validateImageTag('dev-e8cc6889')).toThrowError(
    /not a valid semantic version/,
  );

  expect(() => validateImageTag('1.11.1-dev_underscore')).toThrowError(
    /not a valid semantic version/,
  );
  expect(() => validateImageTag('1.11.1-dev.0123')).toThrowError(
    /not a valid semantic version/,
  );

  expect(() => validateImageTag('invalid:tag')).toThrowError(
    /Invalid Docker image tag/,
  );
  expect(() => validateImageTag('invalid/tag')).toThrowError(
    /Invalid Docker image tag/,
  );
});

test('planDevRelease requires workflowRefName to be main', () => {
  const {exec} = makeMockExec();
  expect(() =>
    planDevRelease({
      exec,
      branchInput: 'greg/test',
      workflowRefName: 'feature-branch',
    }),
  ).toThrow(/must be run from main/);
});

test('planDevRelease rejects empty branch', () => {
  const {exec} = makeMockExec();
  expect(() =>
    planDevRelease({
      exec,
      branchInput: '   ',
      workflowRefName: 'main',
    }),
  ).toThrow(/Branch must not be empty/);
});

test('planDevRelease defaults to main and derives 1.11.1-dev-main-<shortSha>', () => {
  const {calls, exec} = makeMockExec();
  const plan = planDevRelease({
    exec,
    workflowRefName: 'main',
  });

  expect(plan).toEqual({
    image_tag: '1.11.1-dev-main-e8cc6889',
    ref: 'main',
    source_sha: dummySha,
  });

  expect(calls).toContainEqual({
    command: 'git',
    args: ['fetch', 'origin', 'main'],
    options: {stdio: 'inherit'},
  });
});

test('planDevRelease plans dev release for feature branch with short SHA suffix', () => {
  const {calls, exec} = makeMockExec();
  const plan = planDevRelease({
    exec,
    branchInput: 'greg/sync-opt',
    workflowRefName: 'main',
  });

  expect(plan).toEqual({
    image_tag: '1.11.1-dev-greg-sync-opt-e8cc6889',
    ref: 'greg/sync-opt',
    source_sha: dummySha,
  });

  expect(calls).toContainEqual({
    command: 'git',
    args: ['fetch', 'origin', 'greg/sync-opt'],
    options: {stdio: 'inherit'},
  });
});

test('planDevRelease accepts optional commitShaInput', () => {
  const specificSha = '1234567890abcdef1234567890abcdef12345678';
  const {calls, exec} = makeMockExec(specificSha);
  const plan = planDevRelease({
    exec,
    branchInput: 'main',
    commitShaInput: specificSha,
    workflowRefName: 'main',
  });

  expect(plan).toEqual({
    image_tag: '1.11.1-dev-main-12345678',
    ref: 'main',
    source_sha: specificSha,
  });

  expect(calls).toContainEqual({
    command: 'git',
    args: ['rev-parse', '--verify', `${specificSha}^{commit}`],
    options: undefined,
  });
});

test('planDevRelease derives next patch for older base commits (e.g. 0.24.0 -> 0.24.1-dev)', () => {
  const {exec} = makeMockExec(dummySha, undefined, '0.24.0');
  const plan = planDevRelease({
    exec,
    branchInput: 'maint/fix',
    workflowRefName: 'main',
  });

  expect(plan.image_tag).toBe('0.24.1-dev-maint-fix-e8cc6889');
});

test('resolveSourceSha rejects option-like branch starting with -', () => {
  const {exec} = makeMockExec();
  expect(() => resolveSourceSha({branch: '--force', exec})).toThrowError(
    'Branch must not start with "-"',
  );
});

test('resolveSourceSha rejects option-like commitSha starting with -', () => {
  const {exec} = makeMockExec();
  expect(() =>
    resolveSourceSha({
      branch: 'main',
      commitSha: '-v',
      exec,
    }),
  ).toThrowError('Commit SHA must not start with "-"');
});

test('resolveSourceSha rejects invalid commitSha format', () => {
  const {exec} = makeMockExec();
  expect(() =>
    resolveSourceSha({
      branch: 'main',
      commitSha: 'not-a-valid-sha!',
      exec,
    }),
  ).toThrowError(/Invalid commit SHA/);
});

test('resolveUniqueShortSha queries git rev-parse with --short and expands on collision', () => {
  const expandedSha = 'e8cc6889fa';
  const {calls, exec} = makeMockExec(dummySha, expandedSha);

  const shortSha = resolveUniqueShortSha(dummySha, exec);
  expect(shortSha).toBe('e8cc6889fa');
  expect(calls).toContainEqual({
    command: 'git',
    args: ['rev-parse', '--short=8', `${dummySha}^{commit}`],
    options: undefined,
  });
});

test('resolveUniqueShortSha throws if git rev-parse fails or returns invalid output', () => {
  const failingExec: Exec = () => {
    throw new Error('git rev-parse failed');
  };
  expect(() => resolveUniqueShortSha(dummySha, failingExec)).toThrow(
    'git rev-parse failed',
  );

  const invalidExec: Exec = () => 'not-a-valid-sha!';
  expect(() => resolveUniqueShortSha(dummySha, invalidExec)).toThrow(
    /Failed to resolve short SHA/,
  );
});

test('planDevRelease incorporates expanded short SHA when git detects collision', () => {
  const expandedSha = 'e8cc6889fa';
  const {exec} = makeMockExec(dummySha, expandedSha);

  const plan = planDevRelease({
    exec,
    branchInput: 'main',
    workflowRefName: 'main',
  });

  expect(plan.image_tag).toBe('1.11.1-dev-main-e8cc6889fa');
});
