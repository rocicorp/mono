// @ts-check

import path from 'path';
import {fileURLToPath} from 'url';
import {build} from 'esbuild';

// @ts-ignore
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function buildESM() {
  return buildInternal({
    format: 'esm',
    entryPoints: [path.join(__dirname, 'src', 'mod.ts')],
    outfile: path.join(__dirname, 'out/reflect-server.js'),
  });
}

function buildExample() {
  return buildInternal({
    format: 'esm',
    entryPoints: [path.join(__dirname, 'example', 'index.ts')],
    outdir: path.join(__dirname, 'out', 'example'),
  });
}

function buildCLI() {
  return buildInternal({
    format: 'esm',
    entryPoints: [path.join(__dirname, 'tool', 'cli.ts')],
    outfile: path.join(__dirname, 'out', 'cli.js'),
  });
}

/**
 * @param {Partial<import("esbuild").BuildOptions>} options
 */
function buildInternal(options) {
  return build({
    bundle: true,
    minify: true,
    target: 'esnext',
    ...options,
  });
}

try {
  // @ts-ignore
  await Promise.all([buildESM(), buildExample(), buildCLI()]);
} catch {
  process.exitCode = 1;
}
