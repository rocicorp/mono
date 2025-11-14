import {existsSync, readFileSync} from 'node:fs';
import * as path from 'node:path';
import {resolve} from 'node:path';
import {type UserConfig} from 'vite';
import {assert} from '../../shared/src/asserts.ts';
import * as workerUrls from '../../zero-cache/src/server/worker-urls.ts';
import {define, external} from './build-common.ts';

function getPackageJSON() {
  const content = readFileSync(resolve('package.json'), 'utf-8');
  return JSON.parse(content);
}

function convertOutPathToSrcPath(outPath: string): string {
  // Convert "zero/src/name" -> "src/name.ts" or "zero-cache/src/..." -> "../zero-cache/src/....ts"
  if (outPath.startsWith('zero-cache/')) {
    return `../${outPath}.ts`;
  }
  return outPath.replace('zero/src/', 'src/') + '.ts';
}

function extractOutPath(path: string): string | undefined {
  const match = path.match(/^\.\/out\/(.+)\.js$/);
  return match?.[1];
}

function extractEntries(
  entries: Record<string, unknown>,
  getEntryName: (key: string, outPath: string) => string,
): Record<string, string> {
  const entryPoints: Record<string, string> = {};

  for (const [key, value] of Object.entries(entries)) {
    const path =
      typeof value === 'string' ? value : (value as {default?: string}).default;

    if (typeof path === 'string') {
      const outPath = extractOutPath(path);
      if (outPath) {
        const entryName = getEntryName(key, outPath);
        entryPoints[entryName] = resolve(convertOutPathToSrcPath(outPath));
      }
    }
  }

  return entryPoints;
}

function getWorkerEntryPoints(): Record<string, string> {
  // Worker files from zero-cache that need to be bundled

  const entryPoints: Record<string, string> = {};

  for (const url of Object.values(workerUrls)) {
    assert(url instanceof URL);

    // get filename, strip extension
    const worker = path.basename(url.pathname);

    // verify that the file exists in the expected place.
    const srcPath = resolve('../zero-cache/src/server/', worker);
    assert(existsSync(srcPath), `Worker source file not found: ${srcPath}`);

    const workerName = worker.replace(/\.ts$/, '');

    const outPath = `zero-cache/src/server/${workerName}`;
    entryPoints[outPath] = resolve(convertOutPathToSrcPath(outPath));
  }

  return entryPoints;
}

function getAllEntryPoints(): Record<string, string> {
  const packageJSON = getPackageJSON();

  return {
    ...extractEntries(packageJSON.exports ?? {}, (key, outPath) =>
      key === '.' ? 'zero/src/zero' : outPath,
    ),
    ...extractEntries(packageJSON.bin ?? {}, (_, outPath) => outPath),
    ...getWorkerEntryPoints(),
  };
}

export const config: UserConfig = {
  logLevel: 'warn',
  build: {
    outDir: 'out',
    emptyOutDir: false,
    sourcemap: true,
    target: 'es2022',
    ssr: true,
    reportCompressedSize: false,
    minify: false,
    rollupOptions: {
      external,
      input: getAllEntryPoints(),
      output: {
        format: 'es',
        entryFileNames: '[name].js',
        chunkFileNames: 'chunks/[name]-[hash].js',
        preserveModules: true,
      },
    },
  },
  define,
  resolve: {
    conditions: ['import', 'module', 'default'],
  },
};
