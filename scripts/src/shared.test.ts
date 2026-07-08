import {mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, expect, test} from 'vitest';
import {
  assertStableZeroVersion,
  assertZeroTagMatchesVersion,
  assertZeroVersion,
  npmZeroVersionExists,
  readReleaseMode,
  writeGithubOutput,
  writeZeroPackageVersion,
  zeroTag,
  type Exec,
  type ExecError,
} from './shared.ts';

const originalGithubOutput = process.env.GITHUB_OUTPUT;

afterEach(() => {
  if (originalGithubOutput === undefined) {
    delete process.env.GITHUB_OUTPUT;
  } else {
    process.env.GITHUB_OUTPUT = originalGithubOutput;
  }
});

test('release mode validation accepts known modes', () => {
  expect(readReleaseMode('canary')).toBe('canary');
  expect(readReleaseMode('stable')).toBe('stable');
  expect(readReleaseMode('head')).toBe('head');
  expect(() => readReleaseMode('latest')).toThrowErrorMatchingInlineSnapshot(
    `[Error: Unsupported release mode latest]`,
  );
});

test('zero version and tag validation', () => {
  expect(() => assertZeroVersion('1.2.3')).not.toThrow();
  expect(() => assertZeroVersion('1.2.3-canary.4')).not.toThrow();
  expect(() => assertZeroVersion('1.2.3-head.202607082153')).not.toThrow();
  expect(() => assertZeroVersion('v1.2.3')).toThrowErrorMatchingInlineSnapshot(
    `[Error: Invalid version v1.2.3]`,
  );
  expect(() =>
    assertZeroVersion('1.2.3-head'),
  ).toThrowErrorMatchingInlineSnapshot(`[Error: Invalid version 1.2.3-head]`);
  expect(() =>
    assertZeroVersion('1.2.3-beta.1'),
  ).toThrowErrorMatchingInlineSnapshot(`[Error: Invalid version 1.2.3-beta.1]`);

  expect(() => assertStableZeroVersion('1.2.3')).not.toThrow();
  expect(() =>
    assertStableZeroVersion('1.2.3-canary.4'),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Expected stable Zero version, got 1.2.3-canary.4]`,
  );
  expect(() =>
    assertStableZeroVersion('1.2.3-head.202607082153'),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Expected stable Zero version, got 1.2.3-head.202607082153]`,
  );

  expect(zeroTag('1.2.3')).toBe('zero/v1.2.3');
  expect(() =>
    assertZeroTagMatchesVersion('zero/v1.2.3', '1.2.3'),
  ).not.toThrow();
  expect(() =>
    assertZeroTagMatchesVersion('zero/v1.2.4', '1.2.3'),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Tag zero/v1.2.4 does not match version 1.2.3]`,
  );
});

test('writeGithubOutput writes key-value outputs', () => {
  const tempDir = mkdtempSync(join(tmpdir(), 'zero-scripts-test-'));
  try {
    const outputPath = join(tempDir, 'github-output');
    process.env.GITHUB_OUTPUT = outputPath;

    writeGithubOutput({tag: 'zero/v1.2.3', version: '1.2.3'});

    expect(readFileSync(outputPath, 'utf8')).toBe(
      'tag=zero/v1.2.3\nversion=1.2.3\n',
    );
  } finally {
    rmSync(tempDir, {recursive: true, force: true});
  }
});

test('writeGithubOutput rejects multiline values', () => {
  const tempDir = mkdtempSync(join(tmpdir(), 'zero-scripts-test-'));
  try {
    process.env.GITHUB_OUTPUT = join(tempDir, 'github-output');

    expect(() =>
      writeGithubOutput({version: '1.2.3\nmalicious=true'}),
    ).toThrowErrorMatchingInlineSnapshot(
      `[Error: Output version contains a newline]`,
    );
  } finally {
    rmSync(tempDir, {recursive: true, force: true});
  }
});

test('writeZeroPackageVersion updates version and records gitHead', () => {
  const tempDir = mkdtempSync(join(tmpdir(), 'zero-scripts-test-'));
  try {
    const packageJsonPath = join(tempDir, 'package.json');
    writeFileSync(
      packageJsonPath,
      `${JSON.stringify({name: '@rocicorp/zero', version: '1.8.0'}, null, 2)}\n`,
    );

    const sourceSha = 'e8cc6889fa6bc2a364e8cb80776991c308601212';
    const previousVersion = writeZeroPackageVersion(
      '1.8.0-head.202607082153',
      sourceSha,
      packageJsonPath,
    );

    expect(previousVersion).toBe('1.8.0');
    expect(JSON.parse(readFileSync(packageJsonPath, 'utf8'))).toEqual({
      name: '@rocicorp/zero',
      version: '1.8.0-head.202607082153',
      gitHead: sourceSha,
    });

    writeZeroPackageVersion('1.8.1', undefined, packageJsonPath);
    expect(JSON.parse(readFileSync(packageJsonPath, 'utf8'))).toEqual({
      name: '@rocicorp/zero',
      version: '1.8.1',
      gitHead: sourceSha,
    });
  } finally {
    rmSync(tempDir, {recursive: true, force: true});
  }
});

test('writeZeroPackageVersion rejects an invalid gitHead', () => {
  expect(() =>
    writeZeroPackageVersion('1.8.0', 'not-a-sha', 'unused/package.json'),
  ).toThrowErrorMatchingInlineSnapshot(`[Error: Invalid gitHead not-a-sha]`);
});

test('npmZeroVersionExists handles existing and missing npm versions', () => {
  const existing: Exec = () => '1.2.3\n';
  expect(npmZeroVersionExists('1.2.3', existing)).toBe(true);

  const missing: Exec = () => {
    throw missingNpmVersionError();
  };
  expect(npmZeroVersionExists('1.2.3', missing)).toBe(false);
});

test('npmZeroVersionExists surfaces unexpected npm failures', () => {
  const exec: Exec = () => {
    const error = new Error('npm failed') as ExecError;
    error.status = 2;
    error.stderr = Buffer.from('registry unavailable');
    throw error;
  };

  expect(() =>
    npmZeroVersionExists('1.2.3', exec),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Could not check npm for @rocicorp/zero@1.2.3: registry unavailable]`,
  );
});

function missingNpmVersionError() {
  const error = new Error('missing') as ExecError;
  error.status = 1;
  error.stderr = Buffer.from('');
  return error;
}
