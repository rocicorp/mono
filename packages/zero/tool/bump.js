#!/usr/bin/env node
//@ts-check

import {execSync} from 'node:child_process';
import * as fs from 'node:fs';
import * as path from 'node:path';

/** @param {string[]} parts */
function basePath(...parts) {
  return path.join(process.cwd(), ...parts);
}

/**
 * @param {string} command
 * @param {{stdio?:'inherit'|'pipe'|undefined, cwd?:string|undefined}|undefined} [options]
 */
function execute(command, options) {
  console.log(`Executing: ${command}`);
  return execSync(command, {stdio: 'inherit', ...options})
    ?.toString()
    ?.trim();
}

/**
 * @param {fs.PathOrFileDescriptor} packagePath
 */
function getPackageData(packagePath) {
  return JSON.parse(fs.readFileSync(packagePath, 'utf8'));
}

/**
 * @param {fs.PathOrFileDescriptor} packagePath
 * @param {any} data
 */
function writePackageData(packagePath, data) {
  fs.writeFileSync(packagePath, JSON.stringify(data, null, 2) + '\n');
}

const newVersion = process.argv[2];

if (!newVersion) {
  console.error('Usage: node bump.js <version>');
  console.error('Example: node bump.js 0.24.0');
  process.exit(1);
}

// Validate version format
if (!/^\d+\.\d+\.\d+$/.test(newVersion)) {
  console.error(`Invalid version format: ${newVersion}`);
  console.error('Version must be in the format X.Y.Z (e.g., 0.24.0)');
  process.exit(1);
}

console.log(`Bumping version to ${newVersion}...`);

// Find git root
const gitRoot = execute('git rev-parse --show-toplevel', {stdio: 'pipe'});

// Update packages/zero/package.json
const ZERO_PACKAGE_JSON_PATH = path.join(
  gitRoot,
  'packages',
  'zero',
  'package.json',
);
const currentPackageData = getPackageData(ZERO_PACKAGE_JSON_PATH);
const oldVersion = currentPackageData.version;

console.log(`Current version: ${oldVersion}`);
console.log(`New version: ${newVersion}`);

currentPackageData.version = newVersion;
writePackageData(ZERO_PACKAGE_JSON_PATH, currentPackageData);
console.log(`✓ Updated packages/zero/package.json`);

// Find all package.json files in the repo (excluding node_modules and the zero package itself)
console.log('');
console.log('Discovering packages that depend on @rocicorp/zero...');
const findCommand = `find ${gitRoot} -name package.json -not -path "*/node_modules/*" -not -path "*/.turbo/*"`;
const packageJsonFiles = execute(findCommand, {stdio: 'pipe'})
  .split('\n')
  .filter(Boolean)
  .filter(p => p !== ZERO_PACKAGE_JSON_PATH);

// Update all packages that depend on @rocicorp/zero
let updatedCount = 0;
for (const packagePath of packageJsonFiles) {
  const data = getPackageData(packagePath);
  let updated = false;

  if (data.dependencies && data.dependencies['@rocicorp/zero']) {
    data.dependencies['@rocicorp/zero'] = newVersion;
    updated = true;
  }

  if (data.devDependencies && data.devDependencies['@rocicorp/zero']) {
    data.devDependencies['@rocicorp/zero'] = newVersion;
    updated = true;
  }

  if (updated) {
    writePackageData(packagePath, data);
    console.log(`✓ Updated ${path.relative(gitRoot, packagePath)}`);
    updatedCount++;
  }
}

console.log(`✓ Updated ${updatedCount} dependent package(s)`);

// Run npm install to update package-lock.json
console.log('Running npm install to update package-lock.json...');
process.chdir(gitRoot);
execute('npm install');

console.log('');
console.log('Committing changes...');
execute(`git add -A`);
execute(`git commit -m "chore(zero): bump version to ${newVersion}"`);

console.log('');
console.log('✓ Version bump complete. Changes have been committed.');
