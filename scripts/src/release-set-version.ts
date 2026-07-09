// oxlint-disable no-console

import {
  assertZeroVersion,
  writeZeroPackageVersion,
  zeroPackageJsonPath,
} from './shared.ts';

const [version] = process.argv.slice(2);
if (!version) {
  throw new Error(`Usage: node release-set-version.ts <version>`);
}
assertZeroVersion(version);

const previousVersion = writeZeroPackageVersion(version);

console.log(`Updated ${zeroPackageJsonPath}: ${previousVersion} -> ${version}`);
