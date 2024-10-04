// @ts-check

import * as esbuild from 'esbuild';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {builtinModules} from 'node:module';
import {makeDefine, sharedOptions} from '../../shared/src/build.js';
import {getExternalFromPackageJSON} from '../../shared/src/tool/get-external-from-package-json.js';

/**
 * @param {string} path
 * @returns {string}
 */
function basePath(path) {
  return new URL('../' + path, import.meta.url).pathname;
}

const extraExternals = [
  'node:*',
  ...builtinModules,
  // better-sqlite3 is installed using a preinstall script.
  'better-sqlite3',
];

const externalSet = new Set([
  ...(await getExternalFromPackageJSON(import.meta.url)),
  ...extraExternals,
]);

/**
 * @param {string} internalPackageName
 */
async function addExternalDepsFor(internalPackageName) {
  for (const dep of await getExternalFromPackageJSON(
    basePath('../' + internalPackageName),
  )) {
    externalSet.add(dep);
  }
}

// Normally we would put the internal packages in devDependencies, but we cant
// do that because zero has a preinstall script that tries to install the
// devDependencies and fails because the internal packages are not published to
// npm.
//
// preinstall has a `--omit=dev` flag, but it does not work with `npm install`
// for whatever reason.
//
// Instead we list the internal packages here.
for (const dep of [
  'btree',
  'datadog',
  'replicache',
  'shared',
  'zero-cache',
  'zero-client',
  'zero-integration-test',
  'zero-protocol',
  'zero-react',
  'zql',
  'zqlite',
]) {
  await addExternalDepsFor(dep);
}

externalSet.delete('replicache');

const external = [...externalSet].sort();

await verifyExternals(external);

/**
 * @param {Iterable<string>} external
 */
async function verifyExternals(external) {
  // Get the dependencies from the package.json file
  const packageJSON = await readFile(basePath('package.json'), 'utf-8');
  const expectedDeps = new Set(external);
  for (const dep of extraExternals) {
    expectedDeps.delete(dep);
  }

  const {dependencies} = JSON.parse(packageJSON);
  const actualDeps = new Set(Object.keys(dependencies));
  assert.deepEqual(
    expectedDeps,
    actualDeps,
    'zero/package.json dependencies do not match the dependencies of the internal packages',
  );
}

async function buildZeroClient() {
  const define = makeDefine('unknown');
  const entryPoints = {
    zero: basePath('src/zero.ts'),
    react: basePath('src/react.ts'),
  };
  await esbuild.build({
    ...sharedOptions(false, false),
    external,
    splitting: true,
    // Use neutral to remove the automatic define for process.env.NODE_ENV
    platform: 'neutral',
    define,
    outdir: basePath('out'),
    entryPoints,
  });
}

await buildZeroClient();
