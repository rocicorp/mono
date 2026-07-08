// oxlint-disable no-console

import {
  assertGitSha,
  assertZeroTagMatchesVersion,
  assertZeroVersion,
  defaultExec,
  readReleaseMode,
  writeZeroPackageVersion,
  zeroPackageJsonPath,
  type Exec,
  type ReleaseMode,
  type ZeroTag,
  type ZeroVersion,
} from '../shared.ts';

type CreateReleaseTagOptions = {
  exec?: Exec | undefined;
  env?: NodeJS.ProcessEnv | undefined;
  log?: ((message: string) => void) | undefined;
  mode: string;
  sourceSha: string;
  tag: string;
  version: string;
  writeVersion?: ((version: ZeroVersion) => void) | undefined;
};

type CreateReleaseTagArgs = {
  mode: ReleaseMode;
  sourceSha: string;
  tag: ZeroTag;
  version: ZeroVersion;
};

export function runReleaseCreateTagCli() {
  const [mode, version, tag, sourceSha] = process.argv.slice(2);
  if (!mode || !version || !tag || !sourceSha) {
    throw new Error(
      'Usage: node release-create-tag.ts <canary|stable> <version> <tag> <source-sha>',
    );
  }

  createReleaseTag({mode, version, tag, sourceSha});
}

export function createReleaseTag({
  exec = defaultExec,
  env = process.env,
  log = console.log,
  mode: modeArg,
  sourceSha,
  tag,
  version,
  writeVersion = writeZeroPackageVersion,
}: CreateReleaseTagOptions) {
  const args = validateCreateReleaseTagArgs({
    mode: modeArg,
    sourceSha,
    tag,
    version,
  });

  if (args.mode === 'head') {
    // The release workflow skips the tag job for head releases; provenance
    // lives in the npm packument (gitHead) and OCI revision label instead.
    throw new Error('Head releases are not tagged');
  }

  if (args.mode === 'stable') {
    createAndPushTag(exec, log, args.tag, args.sourceSha);
  } else {
    const commit = createCanaryVersionCommit({
      env,
      exec,
      sourceSha: args.sourceSha,
      version: args.version,
      writeVersion,
    });
    log(`Created canary version commit ${commit}`);
    createAndPushTag(exec, log, args.tag, commit);
  }
}

export function validateCreateReleaseTagArgs({
  mode: modeArg,
  sourceSha,
  tag,
  version,
}: {
  mode: string;
  sourceSha: string;
  tag: string;
  version: string;
}): CreateReleaseTagArgs {
  const mode = readReleaseMode(modeArg);
  assertZeroVersion(version);
  assertZeroTagMatchesVersion(tag, version);
  assertGitSha(sourceSha, 'source SHA');
  return {mode, sourceSha, tag, version};
}

function createCanaryVersionCommit({
  env,
  exec,
  sourceSha,
  version,
  writeVersion,
}: {
  env: NodeJS.ProcessEnv;
  exec: Exec;
  sourceSha: string;
  version: ZeroVersion;
  writeVersion: (version: ZeroVersion) => void;
}) {
  writeVersion(version);
  exec('git', ['add', zeroPackageJsonPath], {stdio: 'inherit'});
  const tree = exec('git', ['write-tree']).trim();
  return exec('git', ['commit-tree', tree, '-p', sourceSha], {
    input: `Bump Zero version to ${version}\n`,
    env: {
      ...env,
      GIT_AUTHOR_NAME: 'Rocicorp Release Bot',
      GIT_AUTHOR_EMAIL: 'release-bot@rocicorp.dev',
      GIT_COMMITTER_NAME: 'Rocicorp Release Bot',
      GIT_COMMITTER_EMAIL: 'release-bot@rocicorp.dev',
    },
  }).trim();
}

function createAndPushTag(
  exec: Exec,
  log: (message: string) => void,
  tag: ZeroTag,
  commit: string,
) {
  exec('git', ['tag', tag, commit], {stdio: 'inherit'});
  exec('git', ['push', 'origin', `refs/tags/${tag}`], {stdio: 'inherit'});
  log(`Pushed ${tag} -> ${commit}`);
}
