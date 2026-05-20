import {execSync} from 'node:child_process';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {stdin as input, stdout as output} from 'node:process';
import {createInterface} from 'node:readline/promises';
import commandLineArgs from 'command-line-args';

void main();

async function main() {
  const {mode, from, remote, allowLocalChanges, dockerOnly, yes, dryRun} =
    parseArgs();

  try {
    validateGitArg('ref', from);
    validateGitArg('remote', remote);

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
        console.error(
          `There are uncommitted changes in the working directory.`,
        );
        console.error(`Perhaps you need to commit them?`);
        process.exit(1);
      }
    }

    const fromReleaseVersion = parseReleaseVersionFromTag(from);
    if (mode === 'retry' && fromReleaseVersion === undefined) {
      throw new Error(
        'retry mode requires <from> to be an existing release tag (e.g. zero/v0.24.0-canary.3)',
      );
    }

    // Check that the ref we're building from exists both locally and remotely
    // and that they point to the same commit
    console.log(
      `Verifying ref ${from} exists and matches between local and remote ${remote}...`,
    );

    let localRefHash;

    // Get local ref hash
    try {
      localRefHash = execute(`git rev-parse ${from}`, {stdio: 'pipe'});
    } catch {
      console.error(`Could not resolve local ref: ${from}`);
      console.error(`Make sure the branch/tag exists locally`);
      process.exit(1);
    }

    // For full commit SHAs, skip remote ref lookup — the commit is present
    // locally after a full fetch, which means it was pushed.
    const isCommitSHA = /^[0-9a-f]{40}$/.test(from);
    if (!isCommitSHA) {
      let remoteRefHash;
      try {
        // For branches, check remote/branch
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
          console.error(
            `Make sure the branch/tag has been pushed to ${remote}`,
          );
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
    }

    console.log(
      isCommitSHA
        ? `✓ Commit ${from} exists locally`
        : `✓ Ref ${from} matches between local and remote`,
    );

    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'zero-build-'));

    // Clone from the local repo (uses hardlinks, fast). Then re-point origin
    // at the real remote so that git push/tag operations work correctly.
    console.log(`Cloning repo to ${tempDir}...`);
    execute(`git clone --local ${gitRoot} ${tempDir}`);
    process.chdir(tempDir);

    const remoteUrl = execute(`git -C ${gitRoot} remote get-url ${remote}`, {
      stdio: 'pipe',
    });
    execute(`git remote set-url origin ${remoteUrl}`);
    execute(`git fetch origin --tags`);

    // Try to checkout as remote/branch first, fall back to tag/commit
    try {
      execute(`git checkout origin/${from}`);
    } catch {
      execute(`git checkout ${from}`);
    }

    const zeroPackageJsonPath = basePath('packages', 'zero', 'package.json');
    const packageData = getPackageData(zeroPackageJsonPath);
    const currentVersion = packageData.version;

    let result: Release;
    if (mode === 'canary') {
      result = await releaseCanary(currentVersion, remote, from, yes, dryRun);
    } else if (mode === 'stable') {
      result = await releaseStable(currentVersion, remote, from, yes, dryRun);
    } else {
      if (mode !== 'retry') {
        throw new Error(`Unexpected release mode: ${mode}`);
      }
      result = await retryRelease(
        currentVersion,
        from,
        fromReleaseVersion,
        dockerOnly,
        yes,
        dryRun,
      );
    }

    console.log(``);
    console.log(``);
    console.log(`🎉 ${dryRun ? '[DRY RUN] ' : ''}Success!`);
    console.log(``);
    if (result.pushedGit) {
      console.log(
        `* ${dryRun ? 'Would push' : 'Pushed'} Git tag ${result.tagName} to ${remote}.`,
      );
    }
    if (result.pushedNPM) {
      console.log(
        `* ${dryRun ? 'Would publish' : 'Published'} @rocicorp/zero@${result.version} to npm.`,
      );
    }
    console.log(
      `* ${dryRun ? 'Would create' : 'Created'} Docker image rocicorp/zero:${result.version}.`,
    );
    console.log(``);
    console.log(``);
    console.log(`Next steps:`);
    console.log(``);
    console.log('* Run `git pull --tags` in your checkout to pull the tag.');
    console.log(
      `* Test apps by installing: npm install @rocicorp/zero@${result.version}`,
    );
    if (result.version.includes('-canary.')) {
      console.log('* When ready to promote to stable:');
      console.log(
        `  1. Update base version in package.json if needed: node bump-version.js X.Y.Z`,
      );
      console.log(`  2. Run: node release.ts stable <branch-or-commit>`);
      console.log(
        `  3. When ready for users: npm dist-tag add @rocicorp/zero@X.Y.Z latest`,
      );
    } else {
      console.log('* When ready for users to install:');
      console.log(`  npm dist-tag add @rocicorp/zero@${result.version} latest`);
    }
    console.log(``);
  } catch (error) {
    // oxlint-disable-next-line restrict-template-expressions
    console.error(`Error during execution: ${error}`);
    process.exit(1);
  }
}

type ReleaseMode = 'canary' | 'stable' | 'retry';

type Release = {
  version: string;
  pushedGit: boolean;
  pushedNPM: boolean;
  tagName: string;
};

/** Parse command line arguments */
function parseArgs() {
  const optionDefinitions = [
    {
      name: 'help',
      alias: 'h',
      type: Boolean,
      description: 'Display this usage guide',
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
    {
      name: 'docker-only',
      type: Boolean,
      description: 'Retry mode only: skip npm and publish only docker',
    },
    {
      name: 'yes',
      type: Boolean,
      description: 'Skip interactive confirmation prompt',
    },
    {
      name: 'dry-run',
      type: Boolean,
      description:
        'Build but skip git push, npm publish, and Docker push (for testing)',
    },
    {
      name: 'positionals',
      type: String,
      defaultOption: true,
      multiple: true,
    },
  ];

  let options;
  try {
    options = commandLineArgs(optionDefinitions);
  } catch (e) {
    console.error(`Error: ${String(e)}`);
    showHelp();
    process.exit(1);
  }

  if (options.help) {
    showHelp();
    process.exit(0);
  }

  const positionals = Array.isArray(options.positionals)
    ? options.positionals
    : [];

  if (positionals.length < 2) {
    console.error('Error: Missing required arguments: <mode> <from>');
    showHelp();
    process.exit(1);
  }

  if (positionals.length > 2) {
    console.error(`Error: Unexpected argument ${positionals[2]}`);
    showHelp();
    process.exit(1);
  }

  const modeArg = positionals[0];
  if (modeArg !== 'canary' && modeArg !== 'stable' && modeArg !== 'retry') {
    console.error(`Error: Unknown mode ${modeArg}`);
    showHelp();
    process.exit(1);
  }

  const dockerOnly = Boolean(options['docker-only']);
  if (dockerOnly && modeArg !== 'retry') {
    console.error('--docker-only is only supported with retry mode');
    process.exit(1);
  }

  return {
    mode: modeArg as ReleaseMode,
    from: positionals[1],
    remote: options.remote || 'origin',
    allowLocalChanges: Boolean(options['allow-local-changes']),
    dockerOnly,
    yes: Boolean(options.yes),
    dryRun: Boolean(options['dry-run']),
  };
}

/** Display help message */
function showHelp() {
  console.log(`
Usage: node release.ts <mode> <from> [options]

Creates canary or stable release builds for @rocicorp/zero.

Modes:
  canary             Builds from branch/tag/commit, auto-calculates version from git tags
  stable             Builds from branch/tag/commit using base version from package.json
  retry              Reuses exact tagged version, skips git tag/push

Options:
  -h, --help                 Display this usage guide
  --remote <name>            Git remote to use (default: origin)
  --allow-local-changes      Allow running with local changes in working directory
  --docker-only              Retry mode only: skip npm and publish only docker
  --yes                      Skip interactive confirmation prompt
  --dry-run                  Build but skip git push, npm publish, and Docker push
`);

  console.log(`
Canary Examples:
  node release.ts canary main                          # Build canary from main
  node release.ts canary maint/zero/v0.24              # Build canary from maintenance branch
  node release.ts canary zero/v0.24.0                  # Build new canary from tagged code

Stable Release Examples:
  node release.ts stable main                          # Build stable release from main
  node release.ts stable maint/zero/v0.24              # Build stable release from maintenance branch
  node release.ts stable 4f2c1a9                       # Build stable release from specific commit
  node release.ts stable main --remote upstream        # Build stable release against non-origin remote

Retry Examples:
  node release.ts retry zero/v0.24.0-canary.3                 # Retry npm and docker from existing tag
  node release.ts retry --docker-only zero/v0.24.0-canary.3   # Retry just docker

Maintenance/cherry-pick workflow:
  1. Create a maintenance branch from tag: git checkout -b maint/zero/v0.24 zero/v0.24.0
  2. Cherry-pick commits: git cherry-pick -x <commit-hash>
  3. Push to origin: git push origin maint/zero/v0.24
  4. Run: node release.ts canary maint/zero/v0.24
`);
}

function parseReleaseVersionFromTag(ref: string) {
  const match = ref.match(/^zero\/v(\d+\.\d+\.\d+(?:-canary\.\d+)?)$/);
  return match?.[1];
}

function validateGitArg(name: string, value: string) {
  if (
    value === '' ||
    value.startsWith('-') ||
    value.includes('..') ||
    value.includes('//') ||
    value.includes('@{') ||
    value.endsWith('/') ||
    value.endsWith('.') ||
    value.endsWith('.lock') ||
    !/^[A-Za-z0-9._/-]+$/.test(value)
  ) {
    throw new Error(`Invalid ${name}: ${value}`);
  }
}

async function releaseCanary(
  currentVersion: string,
  remote: string,
  from: string,
  yes: boolean,
  dryRun: boolean,
): Promise<Release> {
  const version = bumpCanaryVersion(currentVersion, remote);
  const tagName = `zero/v${version}`;

  logReleaseHeader(
    `${dryRun ? '[DRY RUN] ' : ''}Creating canary release from ${from}`,
    currentVersion,
    version,
  );
  await confirmRelease(yes || dryRun);

  build(version);
  if (!dryRun) {
    execute(`git commit -am "Bump version to ${version}"`);
  }

  const releaseCommitHash = execute('git rev-parse HEAD', {stdio: 'pipe'});
  if (!releaseCommitHash) {
    throw new Error('Could not resolve HEAD commit for git tag push');
  }
  pushGit(releaseCommitHash, tagName, remote, dryRun);
  pushNpm(version, true, dryRun);
  await pushDocker(version, dryRun);

  return {
    version,
    pushedGit: true,
    pushedNPM: true,
    tagName,
  };
}

async function releaseStable(
  currentVersion: string,
  remote: string,
  from: string,
  yes: boolean,
  dryRun: boolean,
): Promise<Release> {
  const tagName = `zero/v${currentVersion}`;

  logReleaseHeader(
    `${dryRun ? '[DRY RUN] ' : ''}Creating stable release from ${from}`,
    currentVersion,
    currentVersion,
  );
  await confirmRelease(yes || dryRun);

  build(currentVersion);

  const releaseCommitHash = execute('git rev-parse HEAD', {stdio: 'pipe'});
  if (!releaseCommitHash) {
    throw new Error('Could not resolve HEAD commit for git tag push');
  }
  pushGit(releaseCommitHash, tagName, remote, dryRun);
  pushNpm(currentVersion, false, dryRun);
  await pushDocker(currentVersion, dryRun);

  return {
    version: currentVersion,
    pushedGit: true,
    pushedNPM: true,
    tagName,
  };
}

async function retryRelease(
  currentVersion: string,
  from: string,
  fromReleaseVersion: string | undefined,
  dockerOnly: boolean,
  yes: boolean,
  dryRun: boolean,
): Promise<Release> {
  if (fromReleaseVersion === undefined) {
    throw new Error(
      'retry mode requires <from> to be an existing release tag (e.g. zero/v0.24.0-canary.3)',
    );
  }

  const isCanary = fromReleaseVersion.includes('-canary.');
  const tagName = `zero/v${fromReleaseVersion}`;

  logReleaseHeader(
    `${dryRun ? '[DRY RUN] ' : ''}Retrying ${isCanary ? 'canary' : 'stable'} release from ${from}`,
    currentVersion,
    fromReleaseVersion,
    {skipGit: true, skipNPM: dockerOnly},
  );
  await confirmRelease(yes || dryRun);

  if (dockerOnly) {
    console.log('Skipping npm publish (--docker-only)');
  } else {
    build(fromReleaseVersion);
    pushNpm(fromReleaseVersion, isCanary, dryRun);
  }

  await pushDocker(fromReleaseVersion, dryRun);

  return {
    version: fromReleaseVersion,
    pushedGit: false,
    pushedNPM: !dockerOnly,
    tagName,
  };
}

/**
 * @param version - Base version from package.json (e.g., "0.24.0")
 */
function bumpCanaryVersion(version: string, remote: string) {
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

function logReleaseHeader(
  summary: string,
  currentVersion: string,
  nextVersion: string,
  options?: {
    skipGit?: boolean | undefined;
    skipNPM?: boolean | undefined;
  },
) {
  console.log('');
  console.log('='.repeat(60));
  console.log(summary);
  console.log(`Current version: ${currentVersion}`);
  console.log(`Target version:  ${nextVersion}`);
  if (options?.skipGit) {
    console.log(`Git tag/push:    skipped`);
  }
  if (options?.skipNPM) {
    console.log(`npm publish:     skipped`);
  }
  console.log('='.repeat(60));
  console.log('');
}

async function confirmRelease(yes: boolean) {
  if (yes) {
    console.log('Skipping confirmation prompt (--yes)');
    return;
  }

  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    throw new Error(
      'Interactive confirmation required but no TTY is available. Re-run with --yes to skip confirmation.',
    );
  }

  const rl = createInterface({input, output});
  try {
    const answer = (await rl.question('Proceed with release? [y/N] '))
      .trim()
      .toLowerCase();

    if (answer !== 'y' && answer !== 'yes') {
      throw new Error('Release cancelled by user.');
    }
  } finally {
    rl.close();
  }
}

function build(version: string) {
  const usePnpm = fs.existsSync(basePath('pnpm-lock.yaml'));
  const pnpm = (x: string) => execute(usePnpm ? `pnpm ${x}` : `npm ${x}`);
  // Installs turbo and other build dependencies needed for packaging.
  pnpm('install');
  setVersionInWorkspace(version);
  pnpm('install');
  pnpm('run build');
  pnpm('run format');
  pnpm('exec syncpack fix');
  execute('git status');
}

function setVersionInWorkspace(version: string) {
  const zeroPackageJsonPathInTemp = basePath(
    'packages',
    'zero',
    'package.json',
  );
  const currentPackageData = getPackageData(zeroPackageJsonPathInTemp);
  currentPackageData.version = version;
  writePackageData(zeroPackageJsonPathInTemp, currentPackageData);

  const dependencyPaths = [
    basePath('apps', 'zbugs', 'package.json'),
    basePath('apps', 'zql-viz', 'package.json'),
  ];

  dependencyPaths.forEach(p => {
    const data = getPackageData(p);
    if (data.dependencies && data.dependencies['@rocicorp/zero']) {
      data.dependencies['@rocicorp/zero'] = version;
      writePackageData(p, data);
    }
  });
}

function pushGit(
  commitHash: string,
  destTag: string,
  remote: string,
  dryRun: boolean,
) {
  if (dryRun) {
    console.log(
      `[DRY RUN] Would run: git tag ${destTag} ${commitHash} && git push ${remote} refs/tags/${destTag}`,
    );
    return;
  }
  execute(`git tag ${destTag} ${commitHash}`);
  execute(`git push ${remote} refs/tags/${destTag}`);
}

function pushNpm(version: string, isCanary: boolean, dryRun: boolean) {
  if (dryRun) {
    const tag = isCanary ? 'canary' : 'staging';
    console.log(
      `[DRY RUN] Would run: npm publish --provenance --tag=${tag} (then remove staging tag for stable)`,
    );
    return;
  }
  if (isCanary) {
    execute('npm publish --provenance --tag=canary', {
      cwd: basePath('packages', 'zero'),
    });
    return;
  }

  // For stable releases, publish without a dist-tag (we'll add 'latest' separately).
  execute('npm publish --provenance --tag=staging', {
    cwd: basePath('packages', 'zero'),
  });
  execute(`npm dist-tag rm @rocicorp/zero@${version} staging`);
}

async function pushDocker(version: string, dryRun: boolean) {
  if (dryRun) {
    console.log(
      `[DRY RUN] Would run: docker buildx build --platform linux/amd64,linux/arm64 --build-arg=ZERO_VERSION=${version} -t rocicorp/zero:${version} --sbom=true --provenance=mode=max --push .`,
    );
    return;
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
        `docker buildx build \\
    --platform linux/amd64,linux/arm64 \\
    --build-arg=ZERO_VERSION=${version} \\
    -t rocicorp/zero:${version} \\
    --sbom=true \\
    --provenance=mode=max \\
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
}

function execute(
  command: string,
  options?: {stdio?: 'inherit' | 'pipe' | undefined; cwd?: string | undefined},
) {
  console.log(`Executing: ${command}`);
  return execSync(command, {stdio: 'inherit', ...options})
    ?.toString()
    ?.trim();
}

function getPackageData(packagePath: fs.PathOrFileDescriptor) {
  return JSON.parse(fs.readFileSync(packagePath, 'utf8'));
}

function writePackageData(packagePath: fs.PathOrFileDescriptor, data: any) {
  fs.writeFileSync(packagePath, JSON.stringify(data, null, 2));
}

function basePath(...parts: string[]) {
  return path.join(process.cwd(), ...parts);
}
