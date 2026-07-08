// oxlint-disable no-console

import {
  assertZeroVersion,
  writeZeroPackageVersion,
  zeroPackageJsonPath,
} from './shared.ts';

const [version, sourceSha] = process.argv.slice(2);
if (!version) {
  throw new Error(`Usage: node release-set-version.ts <version> [source-sha]`);
}
assertZeroVersion(version);

const previousVersion = writeZeroPackageVersion(
  version,
  sourceSha || undefined,
);

console.log(`Updated ${zeroPackageJsonPath}: ${previousVersion} -> ${version}`);
if (sourceSha) {
  console.log(`Recorded gitHead ${sourceSha}`);
}
