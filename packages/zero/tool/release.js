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
 * @param {string} remote
 */
function bumpCanaryVersion(version, remote) {
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
  console.log(`Fetching tags from remote ${remote}...`);
  execute(`git fetch ${remote} --tags`, {stdio: 'pipe'});

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
 * Find the latest canary tag for a given base version
 * @param {string} baseVersion - e.g., "0.24.0"
 * @param {string} remote
 * @returns {string | null} - e.g., "zero/v0.24.0-canary.5" or null if none found
 */
function findLatestCanaryTag(baseVersion, remote) {
  console.log(
    `Looking for latest canary tag for base version ${baseVersion}...`,
  );
  execute(`git fetch ${remote} --tags`, {stdio: 'pipe'});

  const tagPattern = `zero/v${baseVersion}-canary.*`;
  const tagsOutput = execute(`git tag -l "${tagPattern}"`, {stdio: 'pipe'});

  if (!tagsOutput) {
    return null;
  }

  const tags = tagsOutput.split('\n').filter(Boolean);
  const attemptRegex = new RegExp(
    `^zero/v${baseVersion.replace(/\./g, '\\.')}-canary\\.(\\d+)$`,
  );

  let maxAttempt = -1;
  let latestTag = null;

  for (const tag of tags) {
    const match = tag.match(attemptRegex);
    if (match) {
      const attempt = parseInt(match[1]);
      if (attempt > maxAttempt) {
        maxAttempt = attempt;
        latestTag = tag;
      }
    }
  }

  return latestTag;
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
      type: String,
      description: 'Branch, tag, or commit to build from (required)',
    },
    {
      name: 'stable',
      alias: 's',
      type: Boolean,
      description:
        'Create a stable release using base version. If not provided, creates a canary release (auto-calculated version)',
    },
    {
      name: 'remote',
      type: String,
      description: 'Git remote to use (default: origin)',
    },
    {
      name: 'allow-local-changes',
      type: Boolean,
      description:
        'Allow running with local changes in the working directory (useful for developing script)',
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

  if (!options.from) {
    console.error('Error: --from is required');
    console.error('');
    showHelp(optionDefinitions);
    process.exit(1);
  }

  const isCanary = !Boolean(options.stable);
  const remote = options.remote || 'origin';

  return {
    from: options.from,
    isCanary,
    remote,
    allowLocalChanges: Boolean(options['allow-local-changes']),
  };
}

/**
 * Display help message
 * @param {Array<any>} optionDefinitions
 */
function showHelp(optionDefinitions) {
  console.log(`
Usage: node release.js [options]

Creates canary or stable release builds for @rocicorp/zero.

Default: Creates a canary release (safer, can be repeated)

Modes:
  Canary (default):  Builds from branch/tag/commit, auto-calculates version from git tags
  Stable (--stable): Builds from branch/tag/commit using base version from package.json

Options:`);

  for (const opt of optionDefinitions) {
    const flags = opt.alias ? `-${opt.alias}, --${opt.name}` : `--${opt.name}`;
    console.log(`  ${flags.padEnd(25)} ${opt.description}`);
  }

  console.log(`
Canary Examples:
  node release.js --from main                          # Build canary from main
  node release.js --from maint/zero/v0.24              # Build canary from maintenance branch
  node release.js --from zero/v0.24.0                  # Build canary from specific tag

Stable Release Examples:
  node release.js --stable --from zero/v0.24.0-canary.3    # Promote specific canary to stable

Maintenance/cherry-pick workflow:
  1. Create a maintenance branch from tag: git checkout -b maint/zero/v0.24 zero/v0.24.0
  2. Cherry-pick commits: git cherry-pick <commit-hash>
  3. Push to origin: git push origin maint/zero/v0.24
  4. Run: node release.js --from maint/zero/v0.24
`);
}

const {from: fromArg, isCanary, remote, allowLocalChanges} = parseArgs();

try {
  // Find the git root directory
  const gitRoot = execute('git rev-parse --show-toplevel', {stdio: 'pipe'});

  const remotesOutput = execute('git remote', {stdio: 'pipe'}) ?? '';
  const remotes = remotesOutput.split('\n').filter(Boolean);

  if (!remotes.includes(remote)) {
    console.error(
      `Remote "${remote}" is not configured. Available remotes: ${remotes.join(
        ', ',
      )}`,
    );
    process.exit(1);
  }

  // Check that there are no uncommitted changes
  if (!allowLocalChanges) {
    const uncommittedChanges = execute('git status --porcelain', {
      stdio: 'pipe',
    });
    if (uncommittedChanges) {
      console.error(`There are uncommitted changes in the working directory.`);
      console.error(`Perhaps you need to commit them?`);
      process.exit(1);
    }
  }

  const from = fromArg;

  // For stable releases, we need to know the base version early
  // Read it from the current working directory before we chdir
  const ZERO_PACKAGE_JSON_PATH = path.join(
    gitRoot,
    'packages',
    'zero',
    'package.json',
  );
  const packageData = getPackageData(ZERO_PACKAGE_JSON_PATH);
  const currentVersion = packageData.version;

  // Calculate what the next version will be
  let nextVersion;
  if (isCanary) {
    nextVersion = bumpCanaryVersion(currentVersion, remote);
  } else {
    nextVersion = currentVersion;
  }

  console.log('');
  console.log('='.repeat(60));
  if (isCanary) {
    console.log(`Creating canary release from ${from}`);
  } else {
    console.log(`Creating stable release from ${from}`);
  }
  console.log(`Current version: ${currentVersion}`);
  console.log(`Target version:  ${nextVersion}`);
  console.log('='.repeat(60));
  console.log('');

  // Check that the ref we're building from exists both locally and remotely
  // and that they point to the same commit
  console.log(
    `Verifying ref ${from} exists and matches between local and remote ${remote}...`,
  );

  let localRefHash;
  let remoteRefHash;

  // Get local ref hash
  try {
    localRefHash = execute(`git rev-parse ${from}`, {stdio: 'pipe'});
  } catch {
    console.error(`Could not resolve local ref: ${from}`);
    console.error(`Make sure the branch/tag exists locally`);
    process.exit(1);
  }

  // Get remote ref hash
  try {
    // For branches, check remote/branch
    // For tags, just check the tag (tags are fetched from remote)
    remoteRefHash = execute(`git rev-parse ${remote}/${from}`, {
      stdio: 'pipe',
    });
  } catch {
    // If remote/from doesn't exist, try just the ref (works for tags)
    try {
      // For tags, we need to ensure we have the latest from remote
      execute(`git fetch ${remote} tag ${from}`, {stdio: 'pipe'});
      remoteRefHash = execute(`git rev-parse ${from}`, {stdio: 'pipe'});
    } catch {
      console.error(`Could not resolve remote ref: ${from}`);
      console.error(`Make sure the branch/tag has been pushed to ${remote}`);
      process.exit(1);
    }
  }

  if (localRefHash !== remoteRefHash) {
    console.error(`Local and remote versions of ${from} do not match`);
    console.error(`Local:  ${localRefHash}`);
    console.error(`Remote: ${remoteRefHash}`);
    console.error(`Perhaps you need to push your changes?`);
    process.exit(1);
  }

  console.log(`✓ Ref ${from} matches between local and remote`);

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'zero-build-'));

  // Copy the working directory to temp dir (faster than cloning)
  console.log(`Copying repo from ${gitRoot} to ${tempDir}...`);
  execute(
    `rsync -a --progress --exclude=node_modules --exclude=.turbo ${gitRoot}/ ${tempDir}/`,
  );
  process.chdir(tempDir);

  // Discard any local changes and checkout the correct ref
  execute('git reset --hard');
  execute(`git fetch ${remote}`);

  // Try to checkout as remote/branch first, fall back to tag/commit
  try {
    execute(`git checkout ${remote}/${from}`);
  } catch {
    execute(`git checkout ${from}`);
  }

  //installs turbo and other build dependencies
  execute('npm install');
  // After chdir, use basePath() which is now relative to temp directory
  const ZERO_PACKAGE_JSON_PATH_IN_TEMP = basePath(
    'packages',
    'zero',
    'package.json',
  );
  const currentPackageData = getPackageData(ZERO_PACKAGE_JSON_PATH_IN_TEMP);

  // Update the package.json with the new version
  currentPackageData.version = nextVersion;

  const tagName = `zero/v${nextVersion}`;

  writePackageData(ZERO_PACKAGE_JSON_PATH_IN_TEMP, currentPackageData);

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
  execute('npx syncpack@13 fix-mismatches');

  // Surface information about the code as image metadata (labels) for
  // production / release management.
  const {PROTOCOL_VERSION, MIN_SERVER_SUPPORTED_SYNC_PROTOCOL} =
    await getProtocolVersions();

  execute('git status');

  if (isCanary) {
    execute(`git commit -am "Bump version to ${nextVersion}"`);
  }

  // Push tag to git before npm so that if npm fails the versioning logic works correctly.
  // Also if npm push succeeds but docker fails we correctly record the tag that the
  // npm version was made.
  // Note: We don't merge back to the build branch - canaries are throwaway builds
  // that exist only as tagged commits.
  execute(`git tag ${tagName}`);
  execute(`git push ${remote} ${tagName}`);

  if (isCanary) {
    execute('npm publish --tag=canary', {cwd: basePath('packages', 'zero')});
    execute(`npm dist-tag rm @rocicorp/zero@${nextVersion} canary`);
  } else {
    // For stable releases, publish without a dist-tag (we'll add 'latest' separately)
    execute('npm publish --tag=staging', {cwd: basePath('packages', 'zero')});
    execute(`npm dist-tag rm @rocicorp/zero@${nextVersion} staging`);
  }

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
  console.log(`* Pushed Git tag ${tagName} to ${remote}.`);
  console.log(``);
  console.log(``);
  console.log(`Next steps:`);
  console.log(``);
  console.log('* Run `git pull --tags` in your checkout to pull the tag.');
  console.log(
    `* Test apps by installing: npm install @rocicorp/zero@${nextVersion}`,
  );
  if (isCanary) {
    console.log('* When ready to promote to stable:');
    console.log(
      `  1. Update base version in package.json if needed: node bump-version.js X.Y.Z`,
    );
    console.log(`  2. Run: node release.js --stable --from ${tagName}`);
    console.log(
      `  3. When ready for users: npm dist-tag add @rocicorp/zero@X.Y.Z latest`,
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
