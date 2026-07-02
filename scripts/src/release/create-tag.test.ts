import {expect, test, vi} from 'vitest';
import {type Command, type Exec, type ExecOptions} from '../shared.ts';
import {createReleaseTag, validateCreateReleaseTagArgs} from './create-tag.ts';

const sourceSha = 'e8cc6889fa6bc2a364e8cb80776991c308601212';

test('validateCreateReleaseTagArgs validates mode, version, tag, and source SHA', () => {
  expect(
    validateCreateReleaseTagArgs({
      mode: 'stable',
      sourceSha,
      tag: 'zero/v1.8.0',
      version: '1.8.0',
    }),
  ).toEqual({
    mode: 'stable',
    sourceSha,
    tag: 'zero/v1.8.0',
    version: '1.8.0',
  });

  expect(() =>
    validateCreateReleaseTagArgs({
      mode: 'latest',
      sourceSha,
      tag: 'zero/v1.8.0',
      version: '1.8.0',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Unsupported release mode latest]`,
  );
  expect(() =>
    validateCreateReleaseTagArgs({
      mode: 'stable',
      sourceSha,
      tag: 'zero/v1.8.1',
      version: '1.8.0',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Tag zero/v1.8.1 does not match version 1.8.0]`,
  );
  expect(() =>
    validateCreateReleaseTagArgs({
      mode: 'stable',
      sourceSha: 'not-a-sha',
      tag: 'zero/v1.8.0',
      version: '1.8.0',
    }),
  ).toThrowErrorMatchingInlineSnapshot(`[Error: Invalid source SHA not-a-sha]`);
});

test('createReleaseTag pushes stable tag at the source SHA', () => {
  const {calls, exec} = makeTagExec();
  const log = vi.fn();

  createReleaseTag({
    exec,
    log,
    mode: 'stable',
    sourceSha,
    tag: 'zero/v1.8.0',
    version: '1.8.0',
  });

  expect(calls).toEqual([
    {
      command: 'git',
      args: ['tag', 'zero/v1.8.0', sourceSha],
      options: {stdio: 'inherit'},
    },
    {
      command: 'git',
      args: ['push', 'origin', 'refs/tags/zero/v1.8.0'],
      options: {stdio: 'inherit'},
    },
  ]);
  expect(log).toHaveBeenCalledWith(`Pushed zero/v1.8.0 -> ${sourceSha}`);
});

test('createReleaseTag creates a canary version commit before tagging', () => {
  const {calls, exec} = makeTagExec();
  const log = vi.fn();
  const writeVersion = vi.fn();

  createReleaseTag({
    env: {PATH: '/bin'},
    exec,
    log,
    mode: 'canary',
    sourceSha,
    tag: 'zero/v1.8.0-canary.0',
    version: '1.8.0-canary.0',
    writeVersion,
  });

  expect(writeVersion).toHaveBeenCalledWith('1.8.0-canary.0');
  expect(calls).toEqual([
    {
      command: 'git',
      args: ['add', 'packages/zero/package.json'],
      options: {stdio: 'inherit'},
    },
    {command: 'git', args: ['write-tree'], options: undefined},
    {
      command: 'git',
      args: ['commit-tree', 'tree-sha', '-p', sourceSha],
      options: {
        input: 'Bump Zero version to 1.8.0-canary.0\n',
        env: {
          PATH: '/bin',
          GIT_AUTHOR_NAME: 'Rocicorp Release Bot',
          GIT_AUTHOR_EMAIL: 'release-bot@rocicorp.dev',
          GIT_COMMITTER_NAME: 'Rocicorp Release Bot',
          GIT_COMMITTER_EMAIL: 'release-bot@rocicorp.dev',
        },
      },
    },
    {
      command: 'git',
      args: ['tag', 'zero/v1.8.0-canary.0', 'commit-sha'],
      options: {stdio: 'inherit'},
    },
    {
      command: 'git',
      args: ['push', 'origin', 'refs/tags/zero/v1.8.0-canary.0'],
      options: {stdio: 'inherit'},
    },
  ]);
  expect(log).toHaveBeenCalledWith('Created canary version commit commit-sha');
  expect(log).toHaveBeenCalledWith('Pushed zero/v1.8.0-canary.0 -> commit-sha');
});

function makeTagExec() {
  const calls: Array<{
    command: Command;
    args: readonly string[];
    options: ExecOptions | undefined;
  }> = [];
  const exec: Exec = (command, args, options) => {
    calls.push({command, args, options});
    if (command === 'git' && args[0] === 'write-tree') {
      return 'tree-sha\n';
    }
    if (command === 'git' && args[0] === 'commit-tree') {
      return 'commit-sha\n';
    }
    return '';
  };
  return {calls, exec};
}
