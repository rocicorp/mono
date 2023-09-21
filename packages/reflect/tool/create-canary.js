import * as fs from 'node:fs';
import * as os from 'node:os';
import {execSync} from 'node:child_process';
import * as path from 'path';

/** @param {string[]} parts */
function basePath(...parts) {
  return path.join(process.cwd(), ...parts);
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
  const match = version.match(/-canary\.(\d+)$/);
  if (match) {
    const canaryNum = parseInt(match[1], 10);
    return `${version.split('-canary.')[0]}-canary.${canaryNum + 1}`;
  }
  const [major, minor] = version.split('.');
  return `${major}.${parseInt(minor, 10) + 1}.0-canary.0`;
}

try {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shallow-clone'));
  execute(`git clone --depth 1 git@github.com:rocicorp/mono.git ${tempDir}`);
  process.chdir(tempDir);
  //installs turbo and other build dependencies
  execute('npm install');
  const REFLECT_PACKAGE_JSON_PATH = basePath(
    'packages',
    'reflect',
    'package.json',
  );
  const currentPackageData = getPackageData(REFLECT_PACKAGE_JSON_PATH);
  const nextCanaryVersion = bumpCanaryVersion(currentPackageData.version);
  currentPackageData.version = nextCanaryVersion;

  const tagName = `reflect/v${nextCanaryVersion}`;
  const branchName = `release_reflect/v${nextCanaryVersion}`;
  execute(`git checkout -b ${branchName} origin/main`);

  writePackageData(REFLECT_PACKAGE_JSON_PATH, currentPackageData);

  // publish current canary version so that `npm install` will work down the line
  process.chdir(basePath('packages', 'reflect'));
  execute('npm publish --tag=canary');

  process.chdir(tempDir);
  const dependencyPaths = [
    basePath('apps', 'reflect.net', 'package.json'),
    basePath('mirror', 'mirror-cli', 'package.json'),
  ];

  dependencyPaths.forEach(p => {
    const data = getPackageData(p);
    if (data.dependencies && data.dependencies['@rocicorp/reflect']) {
      data.dependencies['@rocicorp/reflect'] = nextCanaryVersion;
      writePackageData(p, data);
    }
  });

  execute('npm install');
  execute('npm run format');
  execute('npx syncpack');
  execute('git status');
  execute('git add package.json');
  execute('git add **/package.json');
  execute('git add package-lock.json');
  execute(`git commit -m "Bump version to ${nextCanaryVersion}"`);
  execute(`git tag ${tagName}`);
  execute(`git push origin ${tagName}`);
  execute(`git checkout main`);
  execute(`git pull`);
  execute(`git merge ${branchName}`);
  console.log(`please do the following:`);
  console.log(`1. cd ${tempDir}`);
  console.log(
    `2. Please confirm the diff of the commit at HEAD and push to origin if correct`,
  );
  console.log(`3. git diff HEAD^ HEAD`);
  console.log(`4. git push origin main`);
  console.log(`5. cd ~`);
  console.log(`6. rm -rf ${tempDir}`);
} catch (error) {
  console.error(`Error during execution: ${error}`);
  process.exit(1);
}
