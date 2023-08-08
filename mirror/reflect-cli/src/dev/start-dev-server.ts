import type {OutputFile} from 'esbuild';
import {Miniflare} from 'miniflare';
import {nanoid} from 'nanoid';
import * as path from 'node:path';
import {mustFindAppConfigRoot} from '../app-config.js';
import {buildReflectServerContent} from '../compile.js';
import {getWorkerTemplate} from '../get-worker-template.js';
import {inspectorConsoleClient} from './inspector-console-client.js';

/**
 * Returns a function that shuts down the dev server.
 */
export async function startDevServer(
  code: OutputFile,
  sourcemap: OutputFile,
  port: number,
  signal: AbortSignal,
): Promise<URL> {
  const appDir = path.dirname(code.path);
  const appConfigRoot = mustFindAppConfigRoot();
  const inspectorPort = 9229;

  // Create a new Miniflare instance, starting a workerd server
  const mf = new Miniflare({
    port,
    modules: [
      {
        type: 'ESModule',
        path: path.join(appDir, 'worker.js'),
        contents: getWorkerTemplate(
          path.basename(code.path),
          'reflect-server.js',
        ),
      },
      {
        type: 'ESModule',
        path: code.path,
        contents: code.contents,
      },
      {
        type: 'Text',
        path: sourcemap.path,
        contents: sourcemap.contents,
      },
      {
        type: 'ESModule',
        path: path.join(appDir, 'reflect-server.js'),
        contents: await buildReflectServerContent(),
      },
    ],
    bindings: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      REFLECT_AUTH_API_KEY: nanoid(),
    },

    durableObjects: {roomDO: 'RoomDO', authDO: 'AuthDO', testDO: 'TestDO'},

    durableObjectsPersist: path.join(appConfigRoot, '.reflect', 'data'),

    // Use inspector/Chrome DevTools Protocol to forward console.log inside the
    // worker to console.log to the terminal.
    inspectorPort,

    compatibilityDate: '2023-05-18',
  });

  // TODO(arv): When we implement watch mode we need to dispose the workerd instance.
  // workerd itself supports watch but it is not clear how to use it with Miniflare.
  // Cleanup Miniflare, shutting down the workerd server
  // await mf.dispose(),

  const url = await mf.ready;

  await inspectorConsoleClient(url, inspectorPort, signal);

  signal.addEventListener('abort', () => {
    mf.dispose().catch(e => {
      console.error('Failed to shut down dev server', e);
    });
  });

  return url;
}
