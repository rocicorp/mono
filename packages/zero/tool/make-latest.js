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
if (execute('git tag --list latest', {stdio: 'pipe'}) !== '') {
  throw new Error(`Local git tag 'latest' already exists.`);
}
if (
  execute('git ls-remote --tags origin refs/tags/latest', {stdio: 'pipe'}) !==
  ''
) {
  throw new Error(`Remote git tag 'latest' already exists.`);
}
execute(`git tag latest ${gitTag}`);
execute(`git push --no-verify origin refs/tags/latest:refs/tags/latest`);

execute(
  `docker buildx imagetools create -t rocicorp/zero:latest rocicorp/zero:${version}`,
);
execute(`npm dist-tag add @rocicorp/zero@${version} latest`);

const localLatest = execute('git rev-parse latest', {stdio: 'pipe'});
const remoteLatest = execute('git ls-remote --tags origin refs/tags/latest', {
  stdio: 'pipe',
}).split(/\s+/)[0];
if (localLatest !== remoteLatest) {
  throw new Error(
    `Failed to update remote git latest tag: local=${localLatest}, remote=${remoteLatest}`,
  );
}

const dockerVersionDigest = dockerDigest(`rocicorp/zero:${version}`);
const dockerLatestDigest = dockerDigest('rocicorp/zero:latest');
if (dockerVersionDigest !== dockerLatestDigest) {
  throw new Error(
    `Failed to update Docker latest tag: ${dockerLatestDigest} !== ${dockerVersionDigest}`,
  );
}

const npmLatest = execute('npm view @rocicorp/zero dist-tags.latest', {
  stdio: 'pipe',
});
if (npmLatest !== version) {
  throw new Error(
    `Failed to update npm latest tag: ${npmLatest} !== ${version}`,
  );
}

console.log(``);
console.log(``);
console.log(`🎉 Success!`);
console.log(``);
console.log(`* Added 'latest' tag to @rocicorp/zero@${version} on npm.`);
console.log(`* Added 'latest' tag to rocicorp/zero:${version} on dockerhub.`);
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
