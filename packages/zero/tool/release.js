//@ts-check

import commandLineArgs from 'command-line-args';
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

async function getProtocolVersions() {
  const {PROTOCOL_VERSION, MIN_SERVER_SUPPORTED_SYNC_PROTOCOL} = await import(
    basePath(
      'packages',
      'zero',
      'out',
      'zero-protocol',
      'src',
      'protocol-version.js',
    )
  );
  if (
    typeof PROTOCOL_VERSION !== 'number' ||
    typeof MIN_SERVER_SUPPORTED_SYNC_PROTOCOL !== 'number'
  ) {
    throw new Error(
      'Could not extract protocol versions from protocol-version.js',
    );
  }
  return {PROTOCOL_VERSION, MIN_SERVER_SUPPORTED_SYNC_PROTOCOL};
}

/**
 * @param {string} version - Base version from package.json (e.g., "0.24.0")
 */
function bumpCanaryVersion(version) {
  // Canary versions use the format: major.minor.patch-canary.attempt
  //
  // This ensures that canary versions are treated as prereleases in semver,
  // so users with ^X.Y.Z in their package.json won't accidentally upgrade
  // to untested canary builds.
  //
  // We determine the next attempt number by looking at existing git tags
  // for this version. This works because:
  // 1. Canaries are tagged but not merged back to the build branch
  // 2. Git tags are the permanent record of what was released
  // 3. Multiple canaries can exist for the same base version

  // Parse the base version (strip any existing -canary.N suffix)
  const baseVersionMatch = version.match(/^(\d+\.\d+\.\d+)(?:-canary\.\d+)?$/);
  if (!baseVersionMatch) {
    throw new Error(
      `Cannot parse version: ${version}. Expected format: X.Y.Z or X.Y.Z-canary.N`,
    );
  }
  const baseVersion = baseVersionMatch[1];

  // Fetch tags to ensure we have the latest from remote
  console.log('Fetching tags from remote...');
  execute('git fetch --tags', {stdio: 'pipe'});

  // Find all canary tags for this base version
  const tagPattern = `zero/v${baseVersion}-canary.*`;
  const tagsOutput = execute(`git tag -l "${tagPattern}"`, {stdio: 'pipe'});

  let maxAttempt = -1;
  if (tagsOutput) {
    const tags = tagsOutput.split('\n').filter(Boolean);
    const attemptRegex = new RegExp(
      `^zero/v${baseVersion.replace(/\./g, '\\.')}-canary\\.(\\d+)$`,
    );

    for (const tag of tags) {
      const match = tag.match(attemptRegex);
      if (match) {
        const attempt = parseInt(match[1]);
        if (attempt > maxAttempt) {
          maxAttempt = attempt;
        }
      }
    }
  }

  const nextAttempt = maxAttempt + 1;
  const nextVersion = `${baseVersion}-canary.${nextAttempt}`;

  console.log(
    `Found ${maxAttempt + 1} existing canary tag(s) for v${baseVersion}`,
  );
  console.log(`Next canary version: ${nextVersion}`);

  return nextVersion;
}

/**
 * Parse command line arguments
 */
function parseArgs() {
  const optionDefinitions = [
    {
      name: 'help',
      alias: 'h',
      type: Boolean,
      description: 'Display this usage guide',
    },
    {
      name: 'from',
      alias: 'f',
      type: String,
      defaultValue: 'main',
      description: 'Branch, tag, or commit to build from (default: main)',
    },
    {
      name: 'version',
      alias: 'v',
      type: String,
      description:
        'Version for release (e.g., 0.24.0). If not provided, creates next canary',
    },
  ];

  let options;
  try {
    options = commandLineArgs(optionDefinitions);
  } catch (e) {
    console.error(`Error: ${String(e)}`);
    showHelp(optionDefinitions);
    process.exit(1);
  }

  if (options.help) {
    showHelp(optionDefinitions);
    process.exit(0);
  }

  // If version is provided, it's a release build
  // Otherwise, it's a canary build
  const isRelease = Boolean(options.version);

  return {
    from: options.from,
    version: options.version,
    isRelease,
  };
}

/**
 * Display help message
 * @param {Array<any>} optionDefinitions
 */
function showHelp(optionDefinitions) {
  console.log(`
Usage: node create-canary.js [options]

Creates canary or stable release builds for @rocicorp/zero.

Modes:
  Canary:  Builds from branch/tag/commit, auto-calculates version from git tags
  Release: Builds from branch/tag/commit with explicit version

Options:`);

  for (const opt of optionDefinitions) {
    const flags = opt.alias ? `-${opt.alias}, --${opt.name}` : `--${opt.name}`;
    console.log(`  ${flags.padEnd(25)} ${opt.description}`);
  }

  console.log(`
Canary Examples:
  node create-canary.js
  node create-canary.js --from main
  node create-canary.js -f maint/zero/v0.24
  node create-canary.js -f zero/v0.24.0  # Build canary from specific tag

Release Examples:
  node create-canary.js --version 0.24.0 --from zero/v0.24.0-canary.5
  node create-canary.js -v 0.24.1 -f zero/v0.24.1-canary.3

Maintenance/cherry-pick releases:
  1. Create a maintenance branch from tag: git checkout -b maint/zero/v0.24 zero/v0.24.0
  2. Cherry-pick commits: git cherry-pick <commit-hash>
  3. Push to origin: git push origin maint/zero/v0.24
  4. Run: node create-canary.js --from maint/zero/v0.24
`);
}

const {from, version: explicitVersion, isRelease} = parseArgs();

if (isRelease) {
  console.log(`Creating release ${explicitVersion} from ${from}`);
} else {
  console.log(`Creating canary from ${from}`);
}

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

  // Check that local and remote heads match
  // This ensures we're building from code that exists on origin
  const rootHash = execute('git rev-parse HEAD', {stdio: 'pipe'});
  let remoteRefHash;
  try {
    // Try to resolve as a remote ref (works for branches like main, maint/zero/v0.24)
    remoteRefHash = execute(`git rev-parse origin/${from}`, {stdio: 'pipe'});
  } catch {
    // If that fails, try to resolve as a tag or commit hash
    try {
      remoteRefHash = execute(`git rev-parse ${from}`, {stdio: 'pipe'});
    } catch {
      console.error(`Could not resolve ref: ${from}`);
      console.error(`Make sure the branch/tag/commit exists`);
      process.exit(1);
    }
  }

  if (rootHash !== remoteRefHash) {
    console.error(`Root hash of working directory does not match ref ${from}`);
    console.error(`Working directory HEAD: ${rootHash}`);
    console.error(`Ref ${from}: ${remoteRefHash}`);
    console.error(
      `Perhaps you need to push your changes or checkout the correct ref?`,
    );
    process.exit(1);
  }

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'zero-build-'));
  // Find the git root directory
  const gitRoot = execute('git rev-parse --show-toplevel', {stdio: 'pipe'});

  // Copy the working directory to temp dir (faster than cloning)
  console.log(`Copying repo from ${gitRoot} to ${tempDir}...`);
  execute(
    `rsync -a --progress --exclude=node_modules --exclude=.turbo ${gitRoot}/ ${tempDir}/`,
  );
  process.chdir(tempDir);

  // Discard any local changes and checkout the correct ref
  execute('git reset --hard');
  execute('git fetch origin');

  // Try to checkout as origin/branch first, fall back to tag/commit
  try {
    execute(`git checkout origin/${from}`);
  } catch {
    execute(`git checkout ${from}`);
  }

  //installs turbo and other build dependencies
  execute('npm install');
  const ZERO_PACKAGE_JSON_PATH = basePath('packages', 'zero', 'package.json');
  const currentPackageData = getPackageData(ZERO_PACKAGE_JSON_PATH);

  const nextVersion = isRelease
    ? explicitVersion
    : bumpCanaryVersion(currentPackageData.version);

  console.log(`Current version is ${currentPackageData.version}`);
  console.log(`Next version is ${nextVersion}`);
  currentPackageData.version = nextVersion;

  const tagName = `zero/v${nextVersion}`;

  writePackageData(ZERO_PACKAGE_JSON_PATH, currentPackageData);

  const dependencyPaths = [
    basePath('apps', 'zbugs', 'package.json'),
    basePath('apps', 'zql-viz', 'package.json'),
  ];

  dependencyPaths.forEach(p => {
    const data = getPackageData(p);
    if (data.dependencies && data.dependencies['@rocicorp/zero']) {
      data.dependencies['@rocicorp/zero'] = nextVersion;
      writePackageData(p, data);
    }
  });

  execute('npm install');
  execute('npm run build');
  execute('npm run format');
  execute('npx syncpack fix-mismatches');

  // Surface information about the code as image metadata (labels) for
  // production / release management.
  const {PROTOCOL_VERSION, MIN_SERVER_SUPPORTED_SYNC_PROTOCOL} =
    await getProtocolVersions();

  execute('git status');
  execute(`git commit -am "Bump version to ${nextVersion}"`);

  // Push tag to git before npm so that if npm fails the versioning logic works correctly.
  // Also if npm push succeeds but docker fails we correctly record the tag that the
  // npm version was made.
  // Note: We don't merge back to the build branch - canaries are throwaway builds
  // that exist only as tagged commits.
  execute(`git tag ${tagName}`);
  execute(`git push origin ${tagName}`);

  execute('npm publish --tag=canary', {cwd: basePath('packages', 'zero')});
  execute(`npm dist-tag rm @rocicorp/zero@${nextVersion} canary`);

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
    --build-arg=ZERO_VERSION=${nextVersion} \
    --build-arg=ZERO_SYNC_PROTOCOL_VERSION=${PROTOCOL_VERSION} \
    --build-arg=ZERO_MIN_SUPPORTED_SYNC_PROTOCOL_VERSION=${MIN_SERVER_SUPPORTED_SYNC_PROTOCOL} \
    -t rocicorp/zero:${nextVersion} \
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
  console.log(`* Published @rocicorp/zero@${nextVersion} to npm.`);
  console.log(`* Created Docker image rocicorp/zero:${nextVersion}.`);
  console.log(`* Pushed Git tag ${tagName} to origin.`);
  console.log(``);
  console.log(``);
  console.log(`Next steps:`);
  console.log(``);
  console.log('* Run `git pull --tags` in your checkout to pull the tag.');
  console.log(
    `* Test apps by installing: npm install @rocicorp/zero@${nextVersion}`,
  );
  if (!isRelease) {
    console.log('* When ready to promote to stable:');
    console.log(
      `  1. Run: node create-canary.js --version X.Y.Z --from ${tagName}`,
    );
    console.log(
      `  2. When ready for users: npm dist-tag add @rocicorp/zero@X.Y.Z latest`,
    );
  } else {
    console.log('* When ready for users to install:');
    console.log(`  npm dist-tag add @rocicorp/zero@${nextVersion} latest`);
  }
  console.log(``);
} catch (error) {
  console.error(`Error during execution: ${error}`);
  process.exit(1);
}
