import * as esbuild from 'esbuild';
import {fileURLToPath} from 'url';

export async function buildWorker(worker: string): Promise<string> {
  const entryPoint = fileURLToPath(
    new URL(`../../mirror-workers/src/${worker}/index.ts`, import.meta.url),
  );
  const outfile = `out/${worker}.js`;
  await esbuild.build({
    entryPoints: [entryPoint],
    conditions: ['workerd', 'worker', 'browser'],
    bundle: true,
    outfile,
    external: [],
    platform: 'browser',
    target: 'esnext',
    format: 'esm',
    sourcemap: false,
  });
  return outfile;
}
