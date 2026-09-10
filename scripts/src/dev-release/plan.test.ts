import {expect, test} from 'vitest';
import {type Command, type Exec, type ExecOptions} from '../shared.ts';
import {
  deriveDefaultTag,
  normalizeDevImageTag,
  planDevRelease,
  resolveSourceSha,
  sanitizeBranchName,
  validateImageTag,
} from './plan.ts';

const dummySha = 'e8cc6889fa6bc2a364e8cb80776991c308601212';

function makeMockExec(resolvedSha = dummySha) {
  const calls: Array<{
    command: Command;
    args: readonly string[];
    options?: ExecOptions | undefined;
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

test('sanitizeBranchName strips refs prefix and sanitizes special characters to hyphens', () => {
  expect(sanitizeBranchName('refs/heads/greg/sync-opt')).toBe('greg-sync-opt');
  expect(sanitizeBranchName('refs/pull/123/head')).toBe('123-head');
  expect(sanitizeBranchName('feat/my_cool.branch!')).toBe(
    'feat-my-cool-branch',
  );
  expect(sanitizeBranchName('---messy--branch---')).toBe('messy-branch');
});

test('deriveDefaultTag generates 0.0.0-pr-* or 0.0.0-dev-* SemVer tags', () => {
  expect(deriveDefaultTag(dummySha, dummySha)).toBe('0.0.0-dev-e8cc6889');
  expect(deriveDefaultTag('greg/sync-opt', dummySha)).toBe(
    '0.0.0-pr-greg-sync-opt',
  );
  expect(deriveDefaultTag('pr-1234', dummySha)).toBe('0.0.0-pr-1234');
  expect(deriveDefaultTag('dev-test', dummySha)).toBe('0.0.0-dev-test');
  expect(deriveDefaultTag('0.0.0-pr-test', dummySha)).toBe('0.0.0-pr-test');
});

test('normalizeDevImageTag ensures 0.0.0- prefix for custom tags', () => {
  expect(normalizeDevImageTag('custom-bench-1')).toBe(
    '0.0.0-dev-custom-bench-1',
  );
  expect(normalizeDevImageTag('dev-bench-1')).toBe('0.0.0-dev-bench-1');
  expect(normalizeDevImageTag('pr-1234')).toBe('0.0.0-pr-1234');
  expect(normalizeDevImageTag('0.0.0-dev-mytest')).toBe('0.0.0-dev-mytest');
});

test('validateImageTag accepts valid 0.0.0- SemVer tags and blocks invalid/protected tags', () => {
  expect(() => validateImageTag('0.0.0-pr-1234')).not.toThrow();
  expect(() => validateImageTag('0.0.0-dev-e8cc6889')).not.toThrow();
  expect(() => validateImageTag('0.0.0-pr-greg-sync-opt')).not.toThrow();
  expect(() => validateImageTag('0.0.0-dev-custom-bench-1')).not.toThrow();

  expect(() => validateImageTag('latest')).toThrowError(/protected/);
  expect(() => validateImageTag('HEAD')).toThrowError(/protected/);
  expect(() => validateImageTag('staging')).toThrowError(/protected/);
  expect(() => validateImageTag('canary')).toThrowError(/protected/);

  expect(() => validateImageTag('1.8.0')).toThrowError(
    /must start with "0.0.0-"/,
  );
  expect(() => validateImageTag('v1.8.0')).toThrowError(
    /must start with "0.0.0-"/,
  );
  expect(() => validateImageTag('pr-1234')).toThrowError(
    /must start with "0.0.0-"/,
  );
  expect(() => validateImageTag('dev-e8cc6889')).toThrowError(
    /must start with "0.0.0-"/,
  );

  expect(() => validateImageTag('0.0.0-dev_underscore')).toThrowError(
    /not a valid semantic version/,
  );
  expect(() => validateImageTag('0.0.0-dev.0123')).toThrowError(
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
      targetRef: 'greg/test',
      workflowRefName: 'feature-branch',
    }),
  ).toThrow(/must be run from main/);
});

test('planDevRelease rejects empty target ref', () => {
  const {exec} = makeMockExec();
  expect(() =>
    planDevRelease({
      exec,
      targetRef: '   ',
      workflowRefName: 'main',
    }),
  ).toThrow(/Target ref must not be empty/);
});

test('planDevRelease plans dev release with default 0.0.0-pr-* tag', () => {
  const {calls, exec} = makeMockExec();
  const plan = planDevRelease({
    exec,
    targetRef: 'greg/sync-opt',
    workflowRefName: 'main',
  });

  expect(plan).toEqual({
    image_tag: '0.0.0-pr-greg-sync-opt',
    ref: 'greg/sync-opt',
    source_sha: dummySha,
  });

  expect(calls).toContainEqual({
    command: 'git',
    args: ['fetch', 'origin', 'greg/sync-opt'],
    options: {stdio: 'inherit'},
  });
});

test('planDevRelease accepts custom image tag and normalizes to 0.0.0-dev-*', () => {
  const {exec} = makeMockExec();
  const plan = planDevRelease({
    exec,
    targetRef: dummySha,
    imageTagInput: 'custom-bench-1',
    workflowRefName: 'main',
  });

  expect(plan).toEqual({
    image_tag: '0.0.0-dev-custom-bench-1',
    ref: dummySha,
    source_sha: dummySha,
  });
});

test('planDevRelease rejects protected tags in imageTagInput', () => {
  const {exec} = makeMockExec();
  for (const tag of ['latest', 'HEAD', 'staging', 'canary']) {
    expect(() =>
      planDevRelease({
        exec,
        targetRef: dummySha,
        imageTagInput: tag,
        workflowRefName: 'main',
      }),
    ).toThrowError(/protected/);
  }
});

test('resolveSourceSha rejects option-like refs starting with -', () => {
  const {exec} = makeMockExec();
  expect(() => resolveSourceSha('--force', exec)).toThrowError(
    'Target ref must not start with "-"',
  );
  expect(() => resolveSourceSha('-v', exec)).toThrowError(
    'Target ref must not start with "-"',
  );
});
