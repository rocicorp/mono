// Build script for @rocicorp/zero package
import {spawn} from 'node:child_process';
import {existsSync} from 'node:fs';
import {chmod, copyFile, mkdir, readFile, rm} from 'node:fs/promises';
import {builtinModules} from 'node:module';
import {basename, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {type InlineConfig, build as viteBuild} from 'vite';
import {assert} from '../../shared/src/asserts.ts';
import {makeDefine} from '../../shared/src/build.ts';
import {getExternalFromPackageJSON} from '../../shared/src/tool/get-external-from-package-json.ts';
import * as workerUrls from '../../zero-cache/src/server/worker-urls.ts';

const forBundleSizeDashboard = process.argv.includes('--bundle-sizes');
const watchMode = process.argv.includes('--watch');

async function getExternal(): Promise<string[]> {
  return [
    ...(await getExternalFromPackageJSON(import.meta.url, true)),
    'node:*',
    'expo*',
    '@op-engineering/*',
    'zod',
    ...builtinModules,
  ].sort();
}

const external = await getExternal();

const define = {
  ...makeDefine('unknown'),
  'process.env.DISABLE_MUTATION_RECOVERY': 'true',
};

// Vite config helper functions
async function getPackageJSON() {
  const content = await readFile(resolve('package.json'), 'utf-8');
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
  const baseDir = 'zero-cache/src/server';
  const entryPoints: Record<string, string> = {};

  for (const url of Object.values(workerUrls)) {
    assert(url instanceof URL);

    const worker = basename(url.pathname);

    // verify that the file exists in the expected place.
    const srcPath = resolve('..', baseDir, worker);
    assert(existsSync(srcPath), `Worker source file not found: ${srcPath}`);

    const workerName = worker.replace(/\.ts$/, '');
    const outPath = `${baseDir}/${workerName}`;
    entryPoints[outPath] = resolve(convertOutPathToSrcPath(outPath));
  }

  return entryPoints;
}

async function getAllEntryPoints(): Promise<Record<string, string>> {
  const packageJSON = await getPackageJSON();

  return {
    ...extractEntries(packageJSON.exports ?? {}, (key, outPath) =>
      key === '.' ? 'zero/src/zero' : outPath,
    ),
    ...extractEntries(packageJSON.bin ?? {}, (_, outPath) => outPath),
    ...getWorkerEntryPoints(),
  };
}

const baseConfig: InlineConfig = {
  configFile: false,
  logLevel: 'warn',
  define,
  resolve: {
    conditions: ['import', 'module', 'default'],
  },
  build: {
    outDir: 'out',
    emptyOutDir: false,
    minify: forBundleSizeDashboard,
    sourcemap: true,
    target: 'es2022',
    ssr: true,
    reportCompressedSize: false,
  },
};

async function getViteConfig(): Promise<InlineConfig> {
  return {
    ...baseConfig,
    build: {
      ...baseConfig.build,
      rollupOptions: {
        external,
        input: await getAllEntryPoints(),
        output: {
          format: 'es',
          entryFileNames: '[name].js',
          chunkFileNames: 'chunks/[name]-[hash].js',
          preserveModules: true,
        },
      },
    },
  };
}

// Bundle size dashboard config: single entry, no code splitting, minified
// Uses esbuild's dropLabels to strip BUNDLE_SIZE labeled code blocks
const bundleSizeConfig: InlineConfig = {
  ...baseConfig,
  build: {
    ...baseConfig.build,
    rollupOptions: {
      external,
      input: {
        // Single entry point for bundle size measurement
        zero: resolve(import.meta.dirname, '../src/zero.ts'),
      },
      output: {
        format: 'es',
        entryFileNames: '[name].js',
        // No code splitting for bundle size measurements
        inlineDynamicImports: true,
      },
      treeshake: {
        moduleSideEffects: false,
      },
    },
  },
  esbuild: {
    dropLabels: ['BUNDLE_SIZE'],
  },
};

async function makeBinFilesExecutable() {
  const packageJSON = await getPackageJSON();

  if (packageJSON.bin) {
    for (const binPath of Object.values(packageJSON.bin)) {
      const fullPath = resolve(binPath as string);
      await chmod(fullPath, 0o755);
    }
  }
}

async function copyStaticFiles() {
  // Copy litestream config.yml to output directory
  const relPath = 'zero-cache/src/services/litestream';
  const fileName = 'config.yml';
  const srcDir = resolve('..', relPath);
  const destDir = resolve('out', relPath);
  await mkdir(destDir, {recursive: true});
  await copyFile(resolve(srcDir, fileName), resolve(destDir, fileName));
}

async function runPromise(p: Promise<unknown>, label: string) {
  const start = performance.now();
  await p;
  const end = performance.now();
  console.log(`✓ ${label} completed in ${((end - start) / 1000).toFixed(2)}s`);
}

function exec(cmd: string, name: string) {
  return runPromise(
    new Promise<void>((resolve, reject) => {
      const [command, ...args] = cmd.split(' ');
      const proc = spawn(command, args, {stdio: 'inherit'});
      proc.on('exit', code =>
        code === 0 ? resolve() : reject(new Error(`${name} failed`)),
      );
      proc.on('error', reject);
    }),
    name,
  );
}

function runViteBuild(config: InlineConfig, label: string) {
  return runPromise(viteBuild(config), label);
}

async function build() {
  // Run vite build and tsc in parallel
  const startTime = performance.now();

  // Clean output directory for normal builds (preserve for bundle size dashboard and watch mode)
  if (!forBundleSizeDashboard && !watchMode) {
    await rm(resolve('out'), {recursive: true, force: true});
  }

  if (forBundleSizeDashboard) {
    // For bundle size dashboard, build a single minified bundle
    await runViteBuild(bundleSizeConfig, 'vite build (bundle sizes)');
  } else if (watchMode) {
    // Watch mode: run vite and tsc in watch mode
    const viteConfig = await getViteConfig();
    viteConfig.build = {...viteConfig.build, watch: {}};
    await Promise.all([
      runViteBuild(viteConfig, 'vite build (watch)'),
      exec(
        'tsc -p tsconfig.client.json --watch --preserveWatchOutput',
        'client dts (watch)',
      ),
      exec(
        'tsc -p tsconfig.server.json --watch --preserveWatchOutput',
        'server dts (watch)',
      ),
    ]);
  } else {
    // Normal build: use inline vite config + type declarations
    const viteConfig = await getViteConfig();
    await Promise.all([
      runViteBuild(viteConfig, 'vite build'),
      exec('tsc -p tsconfig.client.json', 'client dts'),
      exec('tsc -p tsconfig.server.json', 'server dts'),
    ]);

    await makeBinFilesExecutable();
    await copyStaticFiles();
  }

  const totalDuration = ((performance.now() - startTime) / 1000).toFixed(2);

  console.log(`\n✓ Build completed in ${totalDuration}s`);
}

const isMain = fileURLToPath(import.meta.url) === resolve(process.argv[1]);

if (isMain) {
  await build();
}
