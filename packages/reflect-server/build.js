// @ts-check
/* eslint-env node, es2022 */

import * as esbuild from 'esbuild';
import {polyfillNode} from 'esbuild-plugin-polyfill-node';
import * as fs from 'node:fs';
import * as path from 'path';
import {sharedOptions} from 'shared/src/build.js';
import {fileURLToPath} from 'url';

const metafile = process.argv.includes('--metafile');

const dirname = path.dirname(fileURLToPath(import.meta.url));

// jest-environment-miniflare looks at the wrangler.toml file which builds the example.
function buildExample() {
  return buildInternal({
    entryPoints: [path.join(dirname, 'example', 'index.ts')],
    outdir: path.join(dirname, 'out', 'example'),
    external: [],
    // miniflare does not yet support "node:diagnostics_channel", even with the "nodejs_compat" flag:
    // https://github.com/cloudflare/miniflare/blob/f919a2eaccf30d63f435154969e4233aa3b9531c/packages/core/src/plugins/node/index.ts#L9
    //
    // Stub it out with a polyfill to get tests working.
    plugins: [
      /** @type {esbuild.Plugin} */ (
        polyfillNode({
          diagnostics_channel: true,
          globals: false,
        })
      ),
      wasmPlugin,
    ],
  });
}

function buildCLI() {
  return buildInternal({
    entryPoints: [path.join(dirname, 'tool', 'cli.ts')],
    outfile: path.join(dirname, 'out', 'cli.js'),
  });
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

/**
 * @param {Partial<import("esbuild").BuildOptions>} options
 */
function buildInternal(options) {
  const shared = sharedOptions(true, metafile);
  return esbuild.build({
    // Remove process.env. It does not exist in CF workers.
    define: {'process.env': '{}'},
    plugins: [wasmPlugin],
    ...shared,
    ...options,
  });
}

function copyScriptTemplates() {
  const dir = fs.opendirSync(`./src/script-templates`);
  for (let file = dir.readSync(); file !== null; file = dir.readSync()) {
    if (file.name.endsWith('-script.ts')) {
      const name = file.name.substring(0, file.name.length - 3);
      const src = `./src/script-templates/${file.name}`;
      const dst = `./out/script-templates/${name}.js`; // TODO: actually compile to js?
      doCopy(dst, src);
    }
  }
}

/**
 * @param {string} dst
 * @param {string} src
 */
function doCopy(dst, src) {
  if (!fs.existsSync(src)) {
    throw new Error(`File does not exist: ${src}.`);
  }
  const dstDir = path.dirname(dst);
  if (!fs.existsSync(dstDir)) {
    fs.mkdirSync(dstDir, {recursive: true});
  }

  fs.copyFileSync(src, dst);
}

try {
  await Promise.all([buildExample(), buildCLI()]);
  copyScriptTemplates();
} catch (e) {
  console.error(e);
  process.exit(1);
}
