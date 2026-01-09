// Build script for @rocicorp/zero-server package
import {spawn} from 'node:child_process';
import {builtinModules} from 'node:module';
import {readFile, rm} from 'node:fs/promises';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {type InlineConfig, build as viteBuild} from 'vite';
import {makeDefine} from '../../shared/src/build.ts';
import {getExternalFromPackageJSON} from '../../shared/src/tool/get-external-from-package-json.ts';

const watchMode = process.argv.includes('--watch');

async function getExternal(): Promise<string[]> {
  return [
    ...(await getExternalFromPackageJSON(import.meta.url, true)),
    'node:*',
    ...builtinModules,
  ].sort();
}

const external = await getExternal();

const define = {
  ...makeDefine('unknown'),
  'process.env.DISABLE_MUTATION_RECOVERY': 'true',
};

async function getPackageJSON() {
  const content = await readFile(resolve('package.json'), 'utf-8');
  return JSON.parse(content);
}

function convertOutPathToSrcPath(outPath: string): string {
  return outPath.replace('zero-server-pkg/src/', 'src/') + '.ts';
}

function extractOutPath(path: string): string | undefined {
  const match = path.match(/^\.\/out\/(.+)\.js$/);
  return match?.[1];
}

async function getAllEntryPoints(): Promise<Record<string, string>> {
  const packageJSON = await getPackageJSON();
  const entryPoints: Record<string, string> = {};

  for (const [key, value] of Object.entries(packageJSON.exports ?? {})) {
    const path =
      typeof value === 'string' ? value : (value as {default?: string}).default;

    if (typeof path === 'string') {
      const outPath = extractOutPath(path);
      if (outPath) {
        const entryName = key === '.' ? 'zero-server-pkg/src/mod' : outPath;
        entryPoints[entryName] = resolve(convertOutPathToSrcPath(outPath));
      }
    }
  }

  return entryPoints;
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
    minify: false,
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
  const startTime = performance.now();

  // Clean output directory for normal builds (preserve for watch mode)
  if (!watchMode) {
    await rm(resolve('out'), {recursive: true, force: true});
  }

  if (watchMode) {
    const viteConfig = await getViteConfig();
    viteConfig.build = {...viteConfig.build, watch: {}};
    await Promise.all([
      runViteBuild(viteConfig, 'vite build (watch)'),
      exec('tsc --watch --preserveWatchOutput', 'dts (watch)'),
    ]);
  } else {
    const viteConfig = await getViteConfig();
    await Promise.all([
      runViteBuild(viteConfig, 'vite build'),
      exec('tsc', 'dts'),
    ]);
  }

  const totalDuration = ((performance.now() - startTime) / 1000).toFixed(2);
  console.log(`\n✓ Build completed in ${totalDuration}s`);
}

const isMain = fileURLToPath(import.meta.url) === resolve(process.argv[1]);

if (isMain) {
  await build();
}
