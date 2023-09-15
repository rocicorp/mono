const {execSync} = require('child_process');
const fs = require('fs');
const path = require('path');

const REFLECT_PACKAGE_JSON_PATH = path.join(__dirname, '..', 'package.json');

// Utility Functions
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
  } else {
    const [major, minor] = version.split('.');
    return `${major}.${parseInt(minor, 10) + 1}.0-canary.0`;
  }
}

// Main Logic
try {
  execute('git pull');
  const tagName = `reflect/v${nextCanaryVersion}`;
  const branchName = `release_reflect/v${nextCanaryVersion}`;
  execute(`git checkout -b ${branchName} origin/main`);

  const currentPackageData = getPackageData(REFLECT_PACKAGE_JSON_PATH);
  const nextCanaryVersion = bumpCanaryVersion(currentPackageData.version);
  currentPackageData.version = nextCanaryVersion;
  writePackageData(REFLECT_PACKAGE_JSON_PATH, currentPackageData);

  // publish current canary version so that `npm install` will work down the line
  execute('npm publish --tag=canary');

  const dependencyPaths = [
    path.join(
      __dirname,
      '..',
      '..',
      '..',
      'apps',
      'reflect.net',
      'package.json',
    ),
    path.join(
      __dirname,
      '..',
      '..',
      '..',
      'mirror',
      'mirror-cli',
      'package.json',
    ),
  ];

  dependencyPaths.forEach(p => {
    const data = getPackageData(p);
    if (data.dependencies && data.dependencies['@rocicorp/reflect']) {
      data.dependencies['@rocicorp/reflect'] = nextCanaryVersion;
      writePackageData(p, data);
    }
  });

  process.chdir(path.join(__dirname, '..', '..', '..'));
  execute('npm install');
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
