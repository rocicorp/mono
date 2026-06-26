//@ts-check

import {execSync} from 'node:child_process';

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

if (process.argv.length < 3) {
  console.error(`Usage: node make-latest.js <npm-version>`);
  process.exit(1);
}

const version = process.argv[2];

const gitTag = `zero/v${version}`;
execute(`git tag --force latest ${gitTag}`);
execute(
  `git push --force --no-verify origin refs/tags/latest:refs/tags/latest`,
);

execute(
  `docker buildx imagetools create -t rocicorp/zero:latest rocicorp/zero:${version}`,
);
execute(
  `docker buildx imagetools create -t ghcr.io/rocicorp/zero:latest ghcr.io/rocicorp/zero:${version}`,
);
execute(`pnpm dist-tag add @rocicorp/zero@${version} latest`);

const localLatest = execute('git rev-parse latest', {stdio: 'pipe'});
const remoteLatest = execute('git ls-remote --tags origin refs/tags/latest', {
  stdio: 'pipe',
}).split(/\s+/)[0];
if (localLatest !== remoteLatest) {
  throw new Error(
    `Failed to update remote git latest tag: local=${localLatest}, remote=${remoteLatest}`,
  );
}

retry(() => {
  const dockerVersionDigest = dockerDigest(`rocicorp/zero:${version}`);
  const dockerLatestDigest = dockerDigest('rocicorp/zero:latest');
  if (dockerVersionDigest !== dockerLatestDigest) {
    throw new Error(
      `Failed to update Docker latest tag: ${dockerLatestDigest} !== ${dockerVersionDigest}`,
    );
  }
});

retry(() => {
  const ghcrVersionDigest = dockerDigest(`ghcr.io/rocicorp/zero:${version}`);
  const ghcrLatestDigest = dockerDigest('ghcr.io/rocicorp/zero:latest');
  if (ghcrVersionDigest !== ghcrLatestDigest) {
    throw new Error(
      `Failed to update GHCR latest tag: ${ghcrLatestDigest} !== ${ghcrVersionDigest}`,
    );
  }
});

retry(() => {
  const npmLatest = execute('pnpm view @rocicorp/zero dist-tags.latest', {
    stdio: 'pipe',
  });
  if (npmLatest !== version) {
    throw new Error(
      `Failed to update pnpm latest tag: ${npmLatest} !== ${version}`,
    );
  }
});

console.log(``);
console.log(``);
console.log(`🎉 Success!`);
console.log(``);
console.log(`* Added 'latest' tag to @rocicorp/zero@${version} on npm.`);
console.log(`* Added 'latest' tag to rocicorp/zero:${version} on dockerhub.`);
console.log(`* Added 'latest' tag to ghcr.io/rocicorp/zero:${version} on GHCR.`);
console.log(`* Added 'latest' git tag pointing to ${gitTag}.`);
console.log(``);
console.log(``);
console.log(`Next steps:`);
console.log(``);
console.log('* Bump version on main if necessary.');
console.log(``);

/**
 * @param {string} image
 */
function dockerDigest(image) {
  const output = execute(`docker buildx imagetools inspect ${image}`, {
    stdio: 'pipe',
  });
  const match = output.match(/^Digest:\s+(sha256:[a-f0-9]+)$/m);
  if (!match) {
    throw new Error(`Unable to find digest for ${image}`);
  }
  return match[1];
}

/**
 * @param {() => void} fn
 */
function retry(fn) {
  /** @type {unknown} */
  let lastError;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      fn();
      return;
    } catch (error) {
      lastError = error;
      if (attempt < 2) {
        console.error(lastError);
        console.log(`Retrying...`);
        sleep(2000);
      }
    }
  }
  throw lastError;
}

/**
 * @param {number} ms
 */
function sleep(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}
