import {expect, test} from 'vitest';
import {type Command, type Exec, type ExecOptions} from '../shared.ts';
import {validatePromotion} from './validate.ts';

test('validatePromotion checks git tag, npm version, and Docker images', () => {
  const calls: Array<{
    command: Command;
    args: readonly string[];
    options: ExecOptions | undefined;
  }> = [];
  const exec: Exec = (command, args, options) => {
    calls.push({command, args, options});
    if (command === 'npm') {
      return '1.8.0\n';
    }
    return '';
  };

  expect(
    validatePromotion({exec, version: '1.8.0', workflowRefName: 'main'}),
  ).toEqual({version: '1.8.0', tag: 'zero/v1.8.0'});

  expect(calls).toEqual([
    {
      command: 'git',
      args: ['rev-parse', '--verify', '--quiet', 'refs/tags/zero/v1.8.0'],
      options: {stdio: 'ignore'},
    },
    {
      command: 'npm',
      args: ['view', '--silent', '@rocicorp/zero@1.8.0', 'version'],
      options: {stdio: ['ignore', 'pipe', 'pipe']},
    },
    {
      command: 'docker',
      args: [
        'buildx',
        'imagetools',
        'inspect',
        'docker.io/rocicorp/zero:1.8.0',
      ],
      options: {stdio: 'inherit'},
    },
    {
      command: 'docker',
      args: ['buildx', 'imagetools', 'inspect', 'ghcr.io/rocicorp/zero:1.8.0'],
      options: {stdio: 'inherit'},
    },
  ]);
});

test('validatePromotion rejects non-main workflow refs and canary versions', () => {
  const exec: Exec = () => {
    throw new Error('should not execute external commands');
  };

  expect(() =>
    validatePromotion({exec, version: '1.8.0', workflowRefName: 'feature'}),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Promote workflow must be run from main, got feature]`,
  );
  expect(() =>
    validatePromotion({
      exec,
      version: '1.8.0-canary.0',
      workflowRefName: 'main',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Promotion requires a stable version, got 1.8.0-canary.0]`,
  );
});

test('validatePromotion surfaces Docker inspection failures', () => {
  const exec: Exec = (command, args) => {
    if (command === 'npm') {
      return '1.8.0\n';
    }
    if (
      command === 'docker' &&
      args.includes('docker.io/rocicorp/zero:1.8.0')
    ) {
      throw new Error('missing docker image');
    }
    return '';
  };

  expect(() =>
    validatePromotion({exec, version: '1.8.0', workflowRefName: 'main'}),
  ).toThrowErrorMatchingInlineSnapshot(`[Error: missing docker image]`);
});
