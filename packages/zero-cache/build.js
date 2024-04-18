// @ts-check
/* eslint-env node, es2022 */

import * as esbuild from 'esbuild';
import * as path from 'path';
import {sharedOptions} from 'shared/src/build.js';
import {fileURLToPath} from 'url';

const dirname = path.dirname(fileURLToPath(import.meta.url));

// jest-environment-miniflare looks at the wrangler.toml file which builds the local miniflare.
function buildMiniflareEnvironment() {
  return buildInternal({
    entryPoints: [path.join(dirname, 'test', 'miniflare-environment.ts')],
    outdir: path.join(dirname, 'out', 'test'),
    external: [],
  });
}

/**
 * @param {Partial<import("esbuild").BuildOptions>} options
 */
function buildInternal(options) {
  const shared = sharedOptions(true);
  return esbuild.build({
    // Remove process.env. It does not exist in CF workers.
    define: {'process.env': '{}'},
    ...shared,
    ...options,
  });
}

try {
  await buildMiniflareEnvironment();
} catch (e) {
  console.error(e);
  process.exit(1);
}
