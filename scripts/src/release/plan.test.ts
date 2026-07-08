import {afterEach, expect, test, vi} from 'vitest';
import {
  type Command,
  type Exec,
  type ExecError,
  type ExecOptions,
} from '../shared.ts';
import {
  isAllowedReleaseBranch,
  planCanaryVersion,
  planHeadVersion,
  planRelease,
  planStableVersion,
} from './plan.ts';

afterEach(() => {
  vi.useRealTimers();
});

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

test('head planning stamps the UTC minute onto the base version', () => {
  const now = new Date('2026-07-08T21:53:45.123Z');
  expect(planHeadVersion('1.8.0', now)).toBe('1.8.0-head.202607082153');
  expect(planHeadVersion('1.8.0-canary.5', now)).toBe(
    '1.8.0-head.202607082153',
  );
  expect(planHeadVersion('1.8.0-head.202601010000', now)).toBe(
    '1.8.0-head.202607082153',
  );
  expect(() =>
    planHeadVersion('not-a-version', now),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Cannot plan head from package version not-a-version. Expected X.Y.Z, X.Y.Z-canary.N, or X.Y.Z-head.N]`,
  );
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

test('planRelease returns head outputs and honors the source SHA override', () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-07-08T21:53:45.123Z'));

  const overrideSha = '1111111111111111111111111111111111111111';
  const {calls, exec} = makePlanExec({packageVersion: '1.8.0'});

  expect(
    planRelease({
      exec,
      mode: 'head',
      releaseBranch: 'main',
      sourceSha: overrideSha,
      workflowRefName: 'main',
    }),
  ).toEqual({
    mode: 'head',
    release_branch: 'main',
    version: '1.8.0-head.202607082153',
    tag: 'zero/v1.8.0-head.202607082153',
    source_sha: overrideSha,
    is_canary: 'false',
  });

  // The pushed commit is used verbatim; origin/main HEAD is never resolved.
  expect(calls).not.toContainEqual(
    expect.objectContaining({
      command: 'git',
      args: ['rev-parse', 'refs/remotes/origin/main'],
    }),
  );
  expect(calls).toContainEqual({
    command: 'git',
    args: ['show', `${overrideSha}:packages/zero/package.json`],
    options: undefined,
  });
});

test('planRelease resolves head from origin when no source SHA is given', () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-07-08T21:53:45.123Z'));

  const plan = planRelease({
    exec: makePlanExec({packageVersion: '1.8.0'}).exec,
    mode: 'head',
    releaseBranch: 'main',
    workflowRefName: 'main',
  });
  expect(plan.source_sha).toBe(sourceSha);
  expect(plan.version).toBe('1.8.0-head.202607082153');
});

test('planRelease rejects head releases from non-main branches and bad overrides', () => {
  expect(() =>
    planRelease({
      exec: makePlanExec().exec,
      mode: 'head',
      releaseBranch: 'maint/zero/v1.8',
      workflowRefName: 'main',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Head releases are only supported from main, got maint/zero/v1.8]`,
  );

  expect(() =>
    planRelease({
      exec: makePlanExec().exec,
      mode: 'head',
      releaseBranch: 'main',
      sourceSha: 'not-a-sha',
      workflowRefName: 'main',
    }),
  ).toThrowErrorMatchingInlineSnapshot(`[Error: Invalid source SHA not-a-sha]`);
});

test('planRelease rejects a head version that already exists on npm', () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-07-08T21:53:45.123Z'));

  expect(() =>
    planRelease({
      exec: makePlanExec({npmVersionExists: true}).exec,
      mode: 'head',
      releaseBranch: 'main',
      workflowRefName: 'main',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: @rocicorp/zero@1.8.0-head.202607082153 already exists on npm]`,
  );
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
