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

execute(
  `docker buildx imagetools create -t rocicorp/zero:latest rocicorp/zero:${version}`,
);
execute(`npm dist-tag add @rocicorp/zero@${version} latest`);

console.log(``);
console.log(``);
console.log(`🎉 Success!`);
console.log(``);
console.log(`* Added 'latest' tag to @rocicorp/zero@${version} on npm.`);
console.log(`* Added 'latest' tag to rocicorp/zero:${version} on dockerhub.`);
console.log(``);
console.log(``);
console.log(`Next steps:`);
console.log(``);
console.log('* Bump version on main if necessary.');
console.log(``);
