import {
  createScriptUploadForm,
  type CfModule,
  type CfWorkerInit,
} from 'cloudflare-api/src/create-script-upload-form.js';
import type {AccountAccess} from 'cloudflare-api/src/resources.js';
import {GlobalScript} from 'cloudflare-api/src/scripts.js';
import * as esbuild from 'esbuild';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'url';

export type WorkerName = 'dispatcher' | 'connections-reporter';

export async function publishWorker(
  account: AccountAccess,
  name: WorkerName,
  init: Omit<CfWorkerInit, 'main' | 'name' | 'compatibility_date'>,
): Promise<void> {
  const main: CfModule = {
    name: `${name}.js`,
    content: await buildWorker(name),
    type: 'esm',
  };

  /* eslint-disable @typescript-eslint/naming-convention */
  const form = createScriptUploadForm({
    name,
    main,
    compatibility_date: '2023-09-04',
    ...init,
  });
  /* eslint-enable @typescript-eslint/naming-convention */

  const result = await new GlobalScript(account, name).upload(
    form,
    new URLSearchParams({
      // eslint-disable-next-line @typescript-eslint/naming-convention
      include_subdomain_availability: 'true',
      // pass excludeScript so the whole body of the
      // script doesn't get included in the response
      excludeScript: 'true',
    }),
  );
  console.log(`Publish result:`, result);
}

async function buildWorker(worker: WorkerName): Promise<string> {
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
  const script = await readFile(outfile, 'utf-8');
  console.log(`Built ${worker}:\n`, script);
  return script;
}
