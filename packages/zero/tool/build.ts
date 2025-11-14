// Build script for @rocicorp/zero package
import {spawn} from 'node:child_process';
import {chmod, readFile, rm} from 'node:fs/promises';
import {resolve} from 'node:path';

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

async function build() {
  // Clean output directory
  await rm('out', {recursive: true, force: true});

  // Run vite build and tsc in parallel
  const startTime = performance.now();

  async function exec(cmd: string, name: string) {
    const start = performance.now();
    const [command, ...args] = cmd.split(' ');
    await new Promise<void>((resolve, reject) => {
      const proc = spawn(command, args, {stdio: 'inherit', shell: false});
      proc.on('exit', code => {
        if (code === 0) {
          resolve();
        } else {
          reject(new Error(`${name} failed with code ${code}`));
        }
      });
      proc.on('error', reject);
    });
    const end = performance.now();
    console.log(`✓ ${name} completed in ${((end - start) / 1000).toFixed(2)}s`);
  }

  if (forBundleSizeDashboard) {
    // For bundle size dashboard, build a single minified bundle
    await exec(
      'vite build --config tool/build-bundle-sizes-config.ts',
      'vite build (bundle sizes)',
    );
  } else {
    // Normal build: vite build + type declarations
    await Promise.all([
      exec('vite build', 'vite build'),
      exec('tsc -p tsconfig.client.json', 'client dts'),
      exec('tsc -p tsconfig.server.json', 'server dts'),
    ]);

    await makeBinFilesExecutable();
  }

  const totalDuration = ((performance.now() - startTime) / 1000).toFixed(2);

  console.log(`\n✓ Build completed in ${totalDuration}s`);
}

if (import.meta.main) {
  await build();
}
