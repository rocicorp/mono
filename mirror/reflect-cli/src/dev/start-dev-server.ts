import type {OutputFile} from 'esbuild';
import {Miniflare} from 'miniflare';
import {nanoid} from 'nanoid';
import * as path from 'node:path';
import {mustFindAppConfigRoot} from '../app-config.js';
import {buildReflectServerContent} from '../compile.js';
import {getWorkerTemplate} from '../get-worker-template.js';

/**
 * Returns a function that shuts down the dev server.
 */
export async function startDevServer(
  code: OutputFile,
  sourcemap: OutputFile,
  port: number,
): Promise<URL> {
  const appDir = path.dirname(code.path);
  const appConfigRoot = mustFindAppConfigRoot();

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

    // debug support is currently not supported so supporting inspecting is not very useful.
    // https://github.com/cloudflare/workerd/issues/371
    // inspectorPort: 9229,

    compatibilityDate: '2023-05-18',
  });

  // TODO(arv): When we implement watch mode we need to dispose the workerd instance.
  // workerd itself supports watch but it is not clear how to use it with Miniflare.
  // Cleanup Miniflare, shutting down the workerd server
  // await mf.dispose(),

  return mf.ready;
}
