import {expect, test} from 'vitest';
import {
  type Command,
  type Exec,
  type ExecError,
  type ExecOptions,
} from '../shared.ts';
import {
  isAllowedReleaseBranch,
  planCanaryVersion,
  planRelease,
  planStableVersion,
} from './plan.ts';

const sourceSha = 'e8cc6889fa6bc2a364e8cb80776991c308601212';

test('release branch validation allows only main and Zero maintenance branches', () => {
  expect(isAllowedReleaseBranch('main')).toBe(true);
  expect(isAllowedReleaseBranch('maint/zero/v1.7')).toBe(true);

  expect(isAllowedReleaseBranch('maint/zero/v1')).toBe(false);
  expect(isAllowedReleaseBranch('feature/release')).toBe(false);
  expect(isAllowedReleaseBranch(sourceSha)).toBe(false);
  expect(isAllowedReleaseBranch('refs/tags/zero/v1.7.0')).toBe(false);
});

test('stable planning requires a stable package version', () => {
  expect(planStableVersion('1.8.0')).toBe('1.8.0');
  expect(() =>
    planStableVersion('1.8.0-canary.0'),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Stable releases require a stable package version, got 1.8.0-canary.0]`,
  );
});

test('canary planning starts at zero and increments existing tags', () => {
  expect(planCanaryVersion('1.8.0', [])).toBe('1.8.0-canary.0');
  expect(
    planCanaryVersion('1.8.0', [
      'zero/v1.8.0-canary.0',
      'zero/v1.8.0-canary.2',
      'zero/v1.8.0-canary.not-a-number',
      'zero/v1.9.0-canary.9',
    ]),
  ).toBe('1.8.0-canary.3');
});

test('planRelease resolves source, checks tags and npm, and returns canary outputs', () => {
  const {calls, exec} = makePlanExec({
    packageVersion: '1.8.0',
    tagList: 'zero/v1.8.0-canary.0\nzero/v1.8.0-canary.1\n',
  });

  expect(
    planRelease({
      exec,
      mode: 'canary',
      releaseBranch: 'main',
      workflowRefName: 'main',
    }),
  ).toEqual({
    mode: 'canary',
    release_branch: 'main',
    version: '1.8.0-canary.2',
    tag: 'zero/v1.8.0-canary.2',
    source_sha: sourceSha,
    is_canary: 'true',
  });

  expect(calls).toContainEqual({
    command: 'git',
    args: [
      'fetch',
      'origin',
      '+refs/heads/main:refs/remotes/origin/main',
      '--tags',
    ],
    options: {stdio: 'inherit'},
  });
  expect(calls).toContainEqual({
    command: 'npm',
    args: ['view', '--silent', '@rocicorp/zero@1.8.0-canary.2', 'version'],
    options: {stdio: ['ignore', 'pipe', 'pipe']},
  });
});

test('planRelease rejects existing git tags and npm versions', () => {
  expect(() =>
    planRelease({
      exec: makePlanExec({gitTagExists: true}).exec,
      mode: 'stable',
      releaseBranch: 'main',
      workflowRefName: 'main',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Git tag zero/v1.8.0 already exists]`,
  );

  expect(() =>
    planRelease({
      exec: makePlanExec({npmVersionExists: true}).exec,
      mode: 'stable',
      releaseBranch: 'main',
      workflowRefName: 'main',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: @rocicorp/zero@1.8.0 already exists on npm]`,
  );
});

test('planRelease enforces workflow ref and release branch restrictions', () => {
  expect(() =>
    planRelease({
      exec: makePlanExec().exec,
      mode: 'stable',
      releaseBranch: 'main',
      workflowRefName: 'feature',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Release workflow must be run from main, got feature]`,
  );

  expect(() =>
    planRelease({
      exec: makePlanExec().exec,
      mode: 'stable',
      releaseBranch: sourceSha,
      workflowRefName: 'main',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Unsupported release branch e8cc6889fa6bc2a364e8cb80776991c308601212. Expected main or maint/zero/vX.Y]`,
  );
});

function makePlanExec({
  gitTagExists = false,
  npmVersionExists = false,
  packageVersion = '1.8.0',
  tagList = '',
}: {
  gitTagExists?: boolean | undefined;
  npmVersionExists?: boolean | undefined;
  packageVersion?: string | undefined;
  tagList?: string | undefined;
} = {}) {
  const calls: Array<{
    command: Command;
    args: readonly string[];
    options: ExecOptions | undefined;
  }> = [];
  const exec: Exec = (command, args, options) => {
    calls.push({command, args, options});

    if (command === 'git' && args[0] === 'fetch') {
      return '';
    }
    if (
      command === 'git' &&
      args[0] === 'rev-parse' &&
      args[1] !== '--verify'
    ) {
      return `${sourceSha}\n`;
    }
    if (command === 'git' && args[0] === 'show') {
      return JSON.stringify({version: packageVersion});
    }
    if (command === 'git' && args[0] === 'tag') {
      return tagList;
    }
    if (
      command === 'git' &&
      args[0] === 'rev-parse' &&
      args[1] === '--verify'
    ) {
      if (gitTagExists) {
        return '';
      }
      throw missingCommandResult();
    }
    if (command === 'npm') {
      if (npmVersionExists) {
        return String(args[2]).replace('@rocicorp/zero@', '');
      }
      throw missingCommandResult();
    }

    throw new Error(`Unexpected command ${command} ${args.join(' ')}`);
  };
  return {calls, exec};
}

function missingCommandResult() {
  const error = new Error('missing') as ExecError;
  error.status = 1;
  error.stderr = Buffer.from('');
  return error;
}
