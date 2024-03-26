// @ts-check

import * as esbuild from 'esbuild';
import * as fs from 'node:fs';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
import {makeDefine, sharedOptions} from '../../shared/src/build.js';
import {getExternalFromPackageJSON} from '../../shared/src/tool/get-external-from-package-json.js';

/** @param {string[]} parts */
function basePath(...parts) {
  return path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    '..',
    ...parts,
  );
}

function copyPackages() {
  for (const name of ['client', 'server', 'shared', 'react']) {
    for (const ext of ['d.ts']) {
      const src = basePath(
        '..',
        `reflect-${name}`,
        'out',
        `reflect-${name + '.' + ext}`,
      );
      const dst = basePath((name === 'shared' ? 'index' : name) + '.' + ext);
      doCopy(dst, src, 'packages/' + name);
    }
  }
}

function copyScriptTemplates() {
  const dir = fs.opendirSync(
    basePath('..', 'reflect-server', 'out', 'script-templates'),
  );
  for (let file = dir.readSync(); file !== null; file = dir.readSync()) {
    const src = basePath(
      '..',
      'reflect-server',
      'out',
      'script-templates',
      file.name,
    );

    const dst = basePath('script-templates', file.name);
    doCopy(dst, src, 'packages/reflect-server');
  }
}

/**
 * @param {string} dst
 * @param {string} src
 * @param {string} name
 */
function doCopy(dst, src, name) {
  if (!fs.existsSync(src)) {
    console.error(
      `File does not exist: ${src}. Make sure to build ${name} first`,
    );
    process.exit(1);
  }
  const dstDir = path.dirname(dst);
  if (!fs.existsSync(dstDir)) {
    fs.mkdirSync(dstDir, {recursive: true});
  }
  fs.copyFileSync(src, dst);
}

function copyReflectCLI() {
  const binDir = basePath('bin');
  fs.rmSync(binDir, {recursive: true, force: true});
  const src = basePath('..', '..', 'mirror', 'reflect-cli', 'out', 'index.mjs');
  const dst = basePath('bin', 'cli.js');
  doCopy(dst, src, 'mirror/reflect-cli');
  const templateSrc = basePath(
    '..',
    '..',
    'mirror',
    'reflect-cli',
    'templates',
  );
  const templateDst = basePath('bin', 'templates');
  fs.cpSync(templateSrc, templateDst, {recursive: true});
}

/**
 * @return {string}
 */
function getReflectVersion() {
  const pkg = fs.readFileSync(basePath('package.json'), 'utf-8');
  return JSON.parse(pkg).version;
}

/**
 * @param {any[]} names
 * @returns {Promise<string[]>}
 */
async function getExternalForPackages(...names) {
  const externals = new Set();
  for (const name of names) {
    for (const dep of await getExternalFromPackageJSON(basePath('..', name))) {
      externals.add(dep);
    }
  }
  return [...externals];
}

const wasmPlugin = {
  name: 'wasm',
  setup(build) {
    // Resolve ".wasm" files to a path with a namespace
    build.onResolve({filter: /\.wasm$/}, args => {
      // If this is the import inside the stub module, import the
      // binary itself. Put the path in the "wasm-binary" namespace
      // to tell our binary load callback to load the binary file.
      if (args.namespace === 'wasm-stub') {
        return {
          path: args.path,
          namespace: 'wasm-binary',
        };
      }

      // Otherwise, generate the JavaScript stub module for this
      // ".wasm" file. Put it in the "wasm-stub" namespace to tell
      // our stub load callback to fill it with JavaScript.
      //
      // Resolve relative paths to absolute paths here since this
      // resolve callback is given "resolveDir", the directory to
      // resolve imports against.
      if (args.resolveDir === '') {
        return; // Ignore unresolvable paths
      }
      return {
        path: path.isAbsolute(args.path)
          ? args.path
          : path.join(args.resolveDir, args.path),
        namespace: 'wasm-stub',
      };
    });

    // Virtual modules in the "wasm-stub" namespace are filled with
    // the JavaScript code for compiling the WebAssembly binary. The
    // binary itself is imported from a second virtual module.
    build.onLoad({filter: /.*/, namespace: 'wasm-stub'}, async args => ({
      contents: `import wasm from ${JSON.stringify(args.path)}
        export default (imports) =>
          WebAssembly.instantiate(wasm, imports).then(
            result => result.instance.exports)`,
    }));

    // Virtual modules in the "wasm-binary" namespace contain the
    // actual bytes of the WebAssembly file. This uses esbuild's
    // built-in "binary" loader instead of manually embedding the
    // binary data inside JavaScript code ourselves.
    build.onLoad({filter: /.*/, namespace: 'wasm-binary'}, async args => ({
      contents: await fs.promises.readFile(args.path),
      loader: 'binary',
    }));
  },
};

async function buildPackages() {
  // let's just never minify for now, it's constantly getting in our and our
  // user's way. When we have an automated way to do both minified and non-
  // minified builds we can re-enable this.
  const minify = false;
  let shared = sharedOptions(minify, false);
  const define = makeDefine('unknown');

  fs.rmSync(basePath('out'), {recursive: true, force: true});

  const external = await getExternalForPackages(
    'reflect-server',
    'reflect-client',
    'reflect-shared',
    'reflect-react',
  );
  external.push('node:diagnostics_channel');

  await esbuild.build({
    ...shared,
    external,
    platform: 'browser',
    define: {
      ...define,
      ['REFLECT_VERSION']: JSON.stringify(getReflectVersion()),
      ['TESTING']: 'false',
    },
    format: 'esm',
    entryPoints: [
      basePath('src', 'client.ts'),
      basePath('src', 'server.ts'),
      basePath('src', 'shared.ts'),
      basePath('src', 'react.ts'),
    ],
    bundle: true,
    outdir: 'out',
    splitting: true,
    plugins: [wasmPlugin],
  });
}

await buildPackages();

copyPackages();

copyReflectCLI();

copyScriptTemplates();
