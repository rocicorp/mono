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
    ],
  });
}

function buildCLI() {
  return buildInternal({
    entryPoints: [path.join(dirname, 'tool', 'cli.ts')],
    outfile: path.join(dirname, 'out', 'cli.js'),
  });
}

/**
 * @param {Partial<import("esbuild").BuildOptions>} options
 */
function buildInternal(options) {
  const shared = sharedOptions(true, metafile);
  return esbuild.build({
    // Remove process.env. It does not exist in CF workers.
    define: {'process.env': '{}'},
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
