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

/**
 * @param {boolean} includePeerDeps
 * @returns {Promise<string[]>}
 */
async function getExternal(includePeerDeps) {
  const externalSet = new Set([
    ...(await getExternalFromPackageJSON(import.meta.url, true)),
    ...extraExternals,
  ]);

  /**
   * @param {string} internalPackageName
   */
  async function addExternalDepsFor(internalPackageName) {
    for (const dep of await getExternalFromPackageJSON(
      basePath('../' + internalPackageName),
      includePeerDeps,
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
    'zero-solid',
    'zero-advanced',
    'zer-schema',
    'zql',
    'zqlite',
  ]) {
    await addExternalDepsFor(dep);
  }

  return [...externalSet].sort();
}

const extraExternals = ['node:*', ...builtinModules];

await verifyDependencies(await getExternal(false));

/**
 * @param {Iterable<string>} external
 */
async function verifyDependencies(external) {
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
    solid: basePath('src/solid.ts'),
    internal: basePath('src/advanced.ts'),
  };
  await esbuild.build({
    ...sharedOptions(false, false),
    external: await getExternal(true),
    splitting: true,
    // Use neutral to remove the automatic define for process.env.NODE_ENV
    platform: 'neutral',
    define,
    outdir: basePath('out'),
    entryPoints,
  });
}

await buildZeroClient();
