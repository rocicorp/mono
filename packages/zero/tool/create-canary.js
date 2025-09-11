//@ts-check

import {execSync} from 'node:child_process';
import * as fs from 'node:fs';
import * as os from 'node:os';
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
  fs.writeFileSync(packagePath, JSON.stringify(data, null, 2));
}

/**
 * @param {string} version
 */
function bumpCanaryVersion(version) {
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
  // major.minor.<year><month><day><hour><minute>
  //
  // But these result in integer patch values that are larger than 32 bits and
  // Bun limits version components to 32 bits. So we now use:
  //
  // major.minor.<year><month><day><counter>
  //
  // The counter can be up to 99, so we can have up to 100 versions per day.
  // If we ever find we need more than 100 releases per day (perhaps automated
  // builds?) we can switch to unix timestamp for the time component, but I
  // prefer not to because the current scheme is human readable.
  //
  // This scheme gets up to roughly the year 4050 before running out of bits,
  // hopefully by then Bun has fixed this limitation.
  //
  // NOTE: We used to also include a build hash, but this was removed because
  // there's no way to include them consistently across docker and npm and this
  // was confusing people.
  // See: https://discord.com/channels/830183651022471199/1325165395015110688/1325585636111155293
  const match = version.match(/^(\d+)\.(\d+)\.(\d{10})/);
  if (!match) {
    throw new Error(`Cannot parse existing version: ${version}`);
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
  return `${major}.${minor}.${patch}`;
}

// To do a maintenance/cherry-pick release:
// - create a maintenance release from tag you want to patch, like
//   `maint/zero/vX.Y`
// - cherry-pick the commit(s) you want into that branch
// - push the branch to origin
// - Run this command with the branch name as the first argument

const buildBranch = process.argv[2] ?? 'main';
console.log(`Releasing from branch: ${buildBranch}`);

try {
  // Check that there are no uncommitted changes
  const uncommittedChanges = execute('git status --porcelain', {
    stdio: 'pipe',
  });
  if (uncommittedChanges) {
    console.error(`There are uncommitted changes in the working directory.`);
    console.error(`Perhaps you need to commit them?`);
    process.exit(1);
  }

  // Check that root hash of working directory is the same as the root hash of the build branch
  const rootHash = execute('git rev-parse HEAD', {stdio: 'pipe'});
  const buildBranchRootHash = execute(`git rev-parse origin/${buildBranch}`, {
    stdio: 'pipe',
  });
  if (rootHash !== buildBranchRootHash) {
    console.error(
      `Root hash of working directory does not match root hash of build branch`,
    );
    console.error(`Root hash: ${rootHash}`);
    console.error(`Build branch root hash: ${buildBranchRootHash}`);
    console.error(`Perhaps you need to push your changes to the build branch?`);
    process.exit(1);
  }

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'zero-build-'));
  // In order to be able to check out the release branch we have to do a full
  // clone. Only do the deep clone in this case though since in the common case
  // we can do releases way faster with a shallow clone.
  const shallow = buildBranch === 'main' ? '--depth 1' : '';
  execute(`git clone ${shallow} git@github.com:rocicorp/mono.git ${tempDir}`);
  process.chdir(tempDir);

  if (buildBranch !== 'main') {
    execute(`git checkout origin/${buildBranch}`);
  }

  //installs turbo and other build dependencies
  execute('npm install');
  const ZERO_PACKAGE_JSON_PATH = basePath('packages', 'zero', 'package.json');
  const currentPackageData = getPackageData(ZERO_PACKAGE_JSON_PATH);
  const nextCanaryVersion = bumpCanaryVersion(currentPackageData.version);
  console.log(`Current version is ${currentPackageData.version}`);
  console.log(`Next version is ${nextCanaryVersion}`);
  currentPackageData.version = nextCanaryVersion;

  const tagName = `zero/v${nextCanaryVersion}`;

  writePackageData(ZERO_PACKAGE_JSON_PATH, currentPackageData);

  const dependencyPaths = [
    basePath('apps', 'zbugs', 'package.json'),
    basePath('apps', 'zql-viz', 'package.json'),
  ];

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
  execute(`git commit -am "Bump version to ${nextCanaryVersion}"`);

  // Do this first in case something landed on head in the meantime.
  // We'll get a conflict here in this case.
  execute(`git push origin HEAD:${buildBranch}`);

  // Push to git before npm so that if npm fails the versioning logic works correctly.
  // Also if npm push succeeds but docker fails we correctly record the tag that the
  // npm version was made
  execute(`git tag ${tagName}`);
  execute(`git push origin ${tagName}`);

  execute('npm publish --tag=canary', {cwd: basePath('packages', 'zero')});
  execute(`npm dist-tag rm @rocicorp/zero@${nextCanaryVersion} canary`);

  try {
    // Check if our specific multiarch builder exists
    const builders = execute('docker buildx ls', {stdio: 'pipe'});
    const hasMultiArchBuilder = builders.includes('zero-multiarch');

    if (!hasMultiArchBuilder) {
      console.log('Setting up multi-architecture builder...');
      execute(
        'docker buildx create --name zero-multiarch --driver docker-container --bootstrap',
      );
    }
    execute('docker buildx use zero-multiarch');
    execute('docker buildx inspect zero-multiarch --bootstrap');
  } catch (e) {
    console.error('Failed to set up Docker buildx:', e);
    throw e;
  }

  for (let i = 0; i < 3; i++) {
    try {
      execute(
        `docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --build-arg=ZERO_VERSION=${nextCanaryVersion} \
    -t rocicorp/zero:${nextCanaryVersion} \
    --push .`,
        {cwd: basePath('packages', 'zero')},
      );
    } catch (e) {
      if (i < 3) {
        console.error(`Error building docker image, retrying in 10 seconds...`);
        await new Promise(resolve => setTimeout(resolve, 10_000));
        continue;
      }
      throw e;
    }
    break;
  }

  console.log(``);
  console.log(``);
  console.log(`🎉 Success!`);
  console.log(``);
  console.log(`* Published @rocicorp/zero@${nextCanaryVersion} to npm.`);
  console.log(`* Created Docker image rocicorp/zero:${nextCanaryVersion}.`);
  console.log(
    `* Pushed Git tag ${tagName} to origin and merged with ${buildBranch}.`,
  );
  console.log(``);
  console.log(``);
  console.log(`Next steps:`);
  console.log(``);
  console.log('* Run `git pull` in your checkout to pull the tag.');
  console.log(`* Test apps by installing new version.`);
  console.log('* When ready, use `npm dist-tags` to switch release to latest.');
  console.log(``);
} catch (error) {
  console.error(`Error during execution: ${error}`);
  process.exit(1);
}
