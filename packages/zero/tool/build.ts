// Build script for @rocicorp/zero package
import {spawn} from 'node:child_process';
import {chmod, copyFile, mkdir, readFile, rm} from 'node:fs/promises';
import {resolve} from 'node:path';
import {build as viteBuild} from 'vite';

const forBundleSizeDashboard = process.argv.includes('--bundle-sizes');

async function getPackageJSON() {
  const content = await readFile(resolve('package.json'), 'utf-8');
  return JSON.parse(content);
}

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
  const parts = 'zero-cache/src/services/litestream/config.yml'.split('/');
  const destDir = resolve('out', ...parts.slice(0, -1));
  await mkdir(destDir, {recursive: true});
  await copyFile(resolve('..', ...parts), resolve('out', ...parts));
}

async function build() {
  // Clean output directory (but not for bundle sizes build which adds to existing out/)
  if (!forBundleSizeDashboard) {
    await rm('out', {recursive: true, force: true});
  }

  // Run vite build and tsc in parallel
  const startTime = performance.now();

  async function exec(cmd: string, name: string) {
    const start = performance.now();
    const [command, ...args] = cmd.split(' ');
    const proc = spawn(command, args, {stdio: 'inherit'});
    await new Promise<void>((resolve, reject) => {
      proc.on('exit', code =>
        code === 0 ? resolve() : reject(new Error(`${name} failed`)),
      );
      proc.on('error', reject);
    });
    const end = performance.now();
    console.log(`✓ ${name} completed in ${((end - start) / 1000).toFixed(2)}s`);
  }

  async function runViteBuild(configPath: string, label: string) {
    const start = performance.now();
    const {default: config} = await import(
      resolve(import.meta.dirname, configPath)
    );
    await viteBuild({...config, configFile: false});
    const end = performance.now();
    console.log(
      `✓ ${label} completed in ${((end - start) / 1000).toFixed(2)}s`,
    );
  }

  if (forBundleSizeDashboard) {
    // For bundle size dashboard, build a single minified bundle
    await runViteBuild(
      'build-bundle-sizes-config.ts',
      'vite build (bundle sizes)',
    );
  } else {
    // Normal build: vite build + type declarations
    await Promise.all([
      runViteBuild('../vite.config.ts', 'vite build'),
      exec('tsc -p tsconfig.client.json', 'client dts'),
      exec('tsc -p tsconfig.server.json', 'server dts'),
    ]);

    await makeBinFilesExecutable();
    await copyStaticFiles();
  }

  const totalDuration = ((performance.now() - startTime) / 1000).toFixed(2);

  console.log(`\n✓ Build completed in ${totalDuration}s`);
}

if (import.meta.main) {
  await build();
}
