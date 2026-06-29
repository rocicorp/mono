// oxlint-disable no-console

import {execFileSync} from 'node:child_process';
import {mkdtempSync, readFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {zeroPackageName} from './shared.ts';

const [tarball, expectedVersion] = process.argv.slice(2);
if (!tarball || !expectedVersion) {
  throw new Error(
    'Usage: node release-verify-tarball.ts <tarball> <expected-version>',
  );
}

const tempDir = mkdtempSync(join(tmpdir(), 'zero-tarball-'));
try {
  execFileSync('tar', ['-xzf', tarball, '-C', tempDir, 'package/package.json']);
  const packageJson = JSON.parse(
    readFileSync(join(tempDir, 'package', 'package.json'), 'utf8'),
  ) as {name?: unknown; version?: unknown};

  if (packageJson.name !== zeroPackageName) {
    throw new Error(`Unexpected package name ${String(packageJson.name)}`);
  }
  if (packageJson.version !== expectedVersion) {
    throw new Error(
      `Unexpected package version ${String(packageJson.version)}, expected ${expectedVersion}`,
    );
  }

  console.log(
    `Verified ${tarball} contains ${zeroPackageName}@${expectedVersion}`,
  );
} finally {
  rmSync(tempDir, {recursive: true, force: true});
}
