// Build script for @rocicorp/zero package
import {spawn} from 'node:child_process';
import {existsSync} from 'node:fs';
import {chmod, copyFile, mkdir, readdir, readFile, rm} from 'node:fs/promises';
import {builtinModules} from 'node:module';
import {basename, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {parse} from 'oxc-parser';
import {type InlineConfig, build as viteBuild} from 'vite';
import {assert} from '../../shared/src/asserts.ts';
import {makeDefine} from '../../shared/src/build.ts';
import * as workerUrls from '../../zero-cache/src/server/worker-urls.ts';

const forBundleSizeDashboard = process.argv.includes('--bundle-sizes');
const watchMode = process.argv.includes('--watch');

// Everything imported by a bare specifier is external; only these modules get
// bundled into the output. All other runtime deps are declared in
// dependencies and loaded from node_modules.
const inlinedModules = new Set([
  // Transform helpers (e.g. helpers/usingCtx for `using` declarations) that
  // rolldown injects as imports. Not a real dependency of ours — must ship
  // inside the bundle.
  '@oxc-project/runtime',
]);

// Integration packages imported by adapter entries (./react, ./solid,
// ./server/adapters/*, ./expo-sqlite, ./op-sqlite). They are intentionally
// undeclared: consumers that use an adapter entry already depend on the
// integration themselves, and they must NOT be peerDependencies — pnpm keys a
// package's installed instance by its resolved peers, so a peer that resolves
// in some workspace packages and not others (react in a web app, pg in a
// server package, expo-sqlite in a React Native app) splits @rocicorp/zero
// into multiple copies. TypeScript then sees distinct module identities and
// shared schema/query-registry types stop unifying across the workspace (e.g.
// defineQueries results collapse to `never`), and `declare module
// '@rocicorp/zero'` augmentations only apply to one copy.
//
// This list is not used for bundling (all bare imports are external); it is
// the allowlist for the post-build check that every bare import in the output
// is either a declared dependency or a known integration.
const allowedUndeclaredImports = new Set([
  '@op-engineering/op-sqlite',
  'drizzle-orm',
  'expo-sqlite',
  'kysely',
  'pg',
  'react',
  'solid-js',
]);

// '@scope/pkg/subpath' -> '@scope/pkg', 'pkg/subpath' -> 'pkg'.
function packageName(id: string): string {
  const parts = id.split('/');
  return id.startsWith('@') ? parts.slice(0, 2).join('/') : parts[0];
}

function external(id: string): boolean {
  // Relative/absolute ids and bundler-virtual ids (\0-prefixed) get bundled.
  if (id.startsWith('.') || id.startsWith('/') || id.startsWith('\0')) {
    return false;
  }
  return !inlinedModules.has(packageName(id));
}

const define = {
  ...makeDefine('unknown'),
  'process.env.DISABLE_MUTATION_RECOVERY': 'true',
  'process.env.DISABLE_REPLICACHE_INDEXES': 'true',
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
    assert(url instanceof URL, 'Expected worker URL to be a URL instance');

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
          preserveModulesRoot: resolve('..'),
        },
      },
    },
  };
}

// Bundle size dashboard config: single entry, no code splitting, minified
// Uses Rolldown's transform.dropLabels to strip BUNDLE_SIZE labeled code blocks
// before bundling so the tree-shaker can eliminate the dead Inspector code.
// Note: the old esbuild.dropLabels approach is silently ignored in Vite 8
// because Vite 8 uses Rolldown's oxc transforms instead of esbuild.
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
        codeSplitting: false,
      },
      treeshake: {
        moduleSideEffects: false,
      },
      transform: {
        dropLabels: ['BUNDLE_SIZE'],
      },
    },
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

function assertNoNodeModulesInOut() {
  const nodeModulesPath = resolve('out', 'node_modules');
  if (existsSync(nodeModulesPath)) {
    throw new Error(
      `Build produced out/node_modules — a third-party package was bundled instead of externalized. Path: ${nodeModulesPath}`,
    );
  }
}

async function collectImportSpecifiers(path: string): Promise<string[]> {
  const source = await readFile(path, 'utf-8');
  const {module} = await parse(path, source, {sourceType: 'module'});

  const specifiers: string[] = [];
  for (const imp of module.staticImports) {
    specifiers.push(imp.moduleRequest.value);
  }
  for (const exp of module.staticExports) {
    for (const entry of exp.entries) {
      if (entry.moduleRequest) {
        specifiers.push(entry.moduleRequest.value);
      }
    }
  }
  for (const imp of module.dynamicImports) {
    // moduleRequest is a span of the argument expression; only string
    // literals can be checked statically.
    const text = source.slice(imp.moduleRequest.start, imp.moduleRequest.end);
    const quote = text[0];
    if ((quote === "'" || quote === '"') && text.endsWith(quote)) {
      specifiers.push(text.slice(1, -1));
    }
  }
  return specifiers;
}

// Every bare import left in the output must be resolvable by consumers:
// a declared dependency, a node builtin, or a known integration package
// (allowedUndeclaredImports). Anything else is a typo or an undeclared
// dependency and would fail at the consumer's runtime.
async function assertBareImportsAreDeclared() {
  const packageJSON = await getPackageJSON();
  const allowed = new Set([
    ...Object.keys(packageJSON.dependencies ?? {}),
    ...allowedUndeclaredImports,
    ...builtinModules,
  ]);

  const outDir = resolve('out');
  const files = (await readdir(outDir, {recursive: true, withFileTypes: true}))
    .filter(entry => entry.isFile() && entry.name.endsWith('.js'))
    .map(entry => resolve(entry.parentPath, entry.name));

  const violations = new Map<string, string[]>();
  await Promise.all(
    files.map(async path => {
      for (const id of await collectImportSpecifiers(path)) {
        if (id.startsWith('.') || id.startsWith('/') || id.startsWith('\0')) {
          continue;
        }
        const name = id.startsWith('node:')
          ? id.slice('node:'.length).split('/')[0]
          : packageName(id);
        if (!allowed.has(name)) {
          violations.set(id, [
            ...(violations.get(id) ?? []),
            path.slice(outDir.length + 1),
          ]);
        }
      }
    }),
  );

  if (violations.size > 0) {
    const details = [...violations]
      .map(([id, files]) => `  '${id}' in ${files.join(', ')}`)
      .join('\n');
    throw new Error(
      `Build output imports undeclared modules:\n${details}\n` +
        `Add them to dependencies in package.json, or to allowedUndeclaredImports ` +
        `in build.ts if consumers provide them.`,
    );
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
    assertNoNodeModulesInOut();
    await assertBareImportsAreDeclared();
  }

  const totalDuration = ((performance.now() - startTime) / 1000).toFixed(2);

  console.log(`\n✓ Build completed in ${totalDuration}s`);
}

const isMain = fileURLToPath(import.meta.url) === resolve(process.argv[1]);

if (isMain) {
  await build();
}
