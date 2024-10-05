//@ts-check

import * as fs from 'node:fs';
import * as os from 'node:os';
import {execSync} from 'node:child_process';
import * as path from 'path';

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
  return execSync(command, {stdio: 'inherit', ...options});
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
  fs.writeFileSync(packagePath, JSON.stringify(data, null, 2));
}

/**
 * @param {string} version
 * @param {string} hash
 */
function bumpCanaryVersion(version, hash) {
  // Why this odd version format?
  //
  // I think it is important that builds be automated because I am very
  // distractable and will screw them up if they aren't. Part of this is that
  // each release should have a unique (as far as npm is concerned) version.
  //
  // Build tags do not change the identity of the release. They aren't
  // comparable - one build tag is not bigger than another. So they don't
  // work for this purpose.
  //
  // Previously we constructed versions of the form:
  //
  // major.minor.<year><month><day><hour><minute>+<hash>
  //
  // But these result in integer patch values that are larger than 32 bits and
  // Bun limits version components to 32 bits. So we now use:
  //
  // major.minor.<year><month><day><counter>+<hash>
  //
  // The counter can be up to 99, so we can have up to 100 versions per day.
  // If we ever find we need more than 100 releases per day (perhaps automated
  // builds?) we can switch to unix timestamp for the time component, but I
  // prefer not to because the current scheme is human readable.
  //
  // This scheme gets up to roughly the year 4050 before running out of bits,
  // hopefully by then Bun has fixed this limitation.
  const match = version.match(/^(\d+)\.(\d+)\.(\d{10})\+/);
  if (!match) {
    throw new Error('Cannot parse existing version');
  }

  const [, major, minor, prevPatch] = match;
  const [year, month, day] = new Date().toISOString().split(/[^\d]/);

  const prevPatchPrefix = prevPatch.substring(0, 8);
  const prevPatchCounter = parseInt(prevPatch.substring(8));
  const newPatchPrefix = `${year}${month}${day}`;

  let newPatchCounter = 0;
  if (prevPatchPrefix === newPatchPrefix) {
    newPatchCounter = prevPatchCounter + 1;
    if (newPatchCounter >= 100) {
      throw new Error('Too many releases in one day');
    }
  } else {
    newPatchCounter = 0;
  }

  const patch = newPatchPrefix + String(newPatchCounter).padStart(2, '0');
  return `${major}.${minor}.${patch}+${hash}`;
}

try {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'zero-build-'));
  execute(`git clone --depth 1 git@github.com:rocicorp/mono.git ${tempDir}`);
  process.chdir(tempDir);
  //installs turbo and other build dependencies
  execute('npm install');
  const ZERO_PACKAGE_JSON_PATH = basePath('packages', 'zero', 'package.json');
  const hash = execute('git rev-parse HEAD', {stdio: 'pipe'})
    .toString()
    .trim()
    .substring(0, 6);
  const currentPackageData = getPackageData(ZERO_PACKAGE_JSON_PATH);
  const nextCanaryVersion = bumpCanaryVersion(currentPackageData.version, hash);
  currentPackageData.version = nextCanaryVersion;

  const tagName = `zero/v${nextCanaryVersion}`;
  const branchName = `release_zero/v${nextCanaryVersion}`;
  execute(`git checkout -b ${branchName} origin/main`);

  writePackageData(ZERO_PACKAGE_JSON_PATH, currentPackageData);

  const dependencyPaths = [basePath('apps', 'zbugs', 'package.json')];

  dependencyPaths.forEach(p => {
    const data = getPackageData(p);
    if (data.dependencies && data.dependencies['@rocicorp/zero']) {
      data.dependencies['@rocicorp/zero'] = nextCanaryVersion;
      writePackageData(p, data);
    }
  });

  execute('npm install');
  execute('npm run build');
  execute('npm run format');
  execute('npx syncpack fix-mismatches');
  execute('git status');
  execute('git add package.json');
  execute('git add **/package.json');
  execute('git add package-lock.json');
  execute(`git commit -m "Bump version to ${nextCanaryVersion}"`);

  execute('npm publish --tag=canary', {cwd: basePath('packages', 'zero')});

  execute(`git tag ${tagName}`);
  execute(`git push origin ${tagName}`);
  execute(`git checkout main`);
  execute(`git pull`);
  execute(`git merge ${branchName}`);
  execute(`git push origin main`);

  console.log(``);
  console.log(``);
  console.log(`🎉 Success!`);
  console.log(``);
  console.log(
    `* Published @rocicorp/zero@${nextCanaryVersion} to npm with tag '@canary'.`,
  );
  console.log(`* Pushed Git tag ${tagName} to origin and merged with main.`);
  console.log(``);
  console.log(``);
  console.log(`Next steps:`);
  console.log(``);
  console.log('* Run `git pull` in your checkout to pull the tag.');
  console.log('* Test apps by installing @canary npm release.');
  console.log('* When ready, use `npm dist-tags` to switch release to main.');
  console.log(``);
} catch (error) {
  console.error(`Error during execution: ${error}`);
  process.exit(1);
}
