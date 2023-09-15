import * as fs from 'node:fs';
import {execSync} from 'node:child_process';
import * as path from 'path';
import {fileURLToPath} from 'node:url';

const REFLECT_PACKAGE_JSON_PATH = basePath('..', 'package.json');

/** @param {string[]} parts */
function basePath(...parts) {
  return path.join(path.dirname(fileURLToPath(import.meta.url)), ...parts);
}

function execute(command) {
  console.log(`Executing: ${command}`);
  return execSync(command, {stdio: 'inherit'});
}

function getPackageData(packagePath) {
  return JSON.parse(fs.readFileSync(packagePath, 'utf8'));
}

function writePackageData(packagePath, data) {
  fs.writeFileSync(packagePath, JSON.stringify(data, null, 2));
}

function bumpCanaryVersion(version) {
  if (/-canary\.\d+$/.test(version)) {
    const canaryNum = parseInt(version.split('-canary.')[1], 10);
    return `${version.split('-canary.')[0]}-canary.${canaryNum + 1}`;
  }
  const [major, minor] = version.split('.');
  return `${major}.${parseInt(minor, 10) + 1}.0-canary.0`;
}

try {
  const currentPackageData = getPackageData(REFLECT_PACKAGE_JSON_PATH);
  const nextCanaryVersion = bumpCanaryVersion(currentPackageData.version);
  currentPackageData.version = nextCanaryVersion;

  execute('git pull');
  const tagName = `reflect/v${nextCanaryVersion}`;
  const branchName = `release_reflect/v${nextCanaryVersion}`;
  execute(`git checkout -b ${branchName} origin/main`);

  writePackageData(REFLECT_PACKAGE_JSON_PATH, currentPackageData);

  // publish current canary version so that `npm install` will work down the line
  execute('npm publish --tag=canary');

  const dependencyPaths = [
    basePath('..', '..', '..', 'apps', 'reflect.net', 'package.json'),
    basePath('..', '..', '..', 'mirror', 'mirror-cli', 'package.json'),
  ];

  dependencyPaths.forEach(p => {
    const data = getPackageData(p);
    if (data.dependencies && data.dependencies['@rocicorp/reflect']) {
      data.dependencies['@rocicorp/reflect'] = nextCanaryVersion;
      writePackageData(p, data);
    }
  });

  process.chdir(basePath('..', '..', '..'));
  execute('npm install');
  execute('npm run format');
  execute('git add **/package.json');
  execute('git add package-lock.json');
  execute(`git commit -m "Bump version to ${nextCanaryVersion}"`);
  execute(`git tag ${tagName}`);
  execute(`git push origin ${tagName}`);
  execute(`git checkout main`);
  execute(`git pull`);
  execute(`git merge ${branchName}`);

  console.log(
    `Please confirm the diff of the commit at HEAD and push to origin if correct`,
  );
} catch (error) {
  console.error(`Error during execution: ${error}`);
  process.exit(1);
}
