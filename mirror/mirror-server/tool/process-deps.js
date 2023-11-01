// @ts-check
import {readFileSync, writeFileSync} from 'fs';

// When deploying to Firebase, Firebase does an `npm install` which tries to
// install our internal packages. That fails because these packages are not
// published to npm. We therefore remove the internal packages from the
// package.json before deploying to firebase.
//
// This is done using predeploy and postdeploy hooks in firebase.json.
//
// We store the list of internal packages in the `bundleDependencies` field of
// package.json. Normally, bundleDependencies is used to specify packages that
// should be bundled into the tarball created by `npm pack`. However, we don't
// use `npm pack` and instead use esbuild to bundle our code. We therefore
// repurpose bundleDependencies to specify packages that should be removed from
// package.json before deploying to Firebase.
//
// Previously we used to not declare these packages as dependencies, but that
// caused problems with turbo repo which failed to build things in the right
// order.

const filename = new URL('../package.json', import.meta.url);
const packageJson = JSON.parse(readFileSync(filename, 'utf-8'));
const {bundleDependencies, dependencies, devDependencies} = packageJson;

function removeDeps() {
  for (const dep of bundleDependencies) {
    delete dependencies[dep];
    delete devDependencies[dep];
  }
}

function restoreDeps() {
  for (const dep of bundleDependencies) {
    devDependencies[dep] = '0.0.0';
  }
}

if (process.argv.includes('--remove')) {
  console.log('Removing dependencies from package.json');
  removeDeps();
} else if (process.argv.includes('--restore')) {
  console.log('Restoring dependencies in package.json');
  restoreDeps();
} else {
  console.error('Please specify --remove or --restore');
  process.exit(1);
}

writeFileSync('./package.json', JSON.stringify(packageJson, null, 2) + '\n');
