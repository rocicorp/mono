// @ts-check
/* eslint-env node, es2022 */

import * as esbuild from 'esbuild';
import * as path from 'path';
import {sharedOptions} from 'shared/src/build.js';
import {fileURLToPath} from 'url';

const dirname = path.dirname(fileURLToPath(import.meta.url));

// @cloudflare/vitest-pool-workers/config needs a js file with the miniflare environment.
function buildMiniflareEnvironment() {
  return buildInternal({
    entryPoints: [path.join(dirname, 'test', 'miniflare-environment.ts')],
    outdir: path.join(dirname, 'out', 'test'),
    external: [],
    platform: 'node',
  });
}

/**
 * @param {import("esbuild").BuildOptions} options
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
