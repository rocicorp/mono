import type {OutputFile} from 'esbuild';
import getPort from 'get-port';
import {Miniflare} from 'miniflare';
import {nanoid} from 'nanoid';
import * as path from 'node:path';
import {mustFindAppConfigRoot} from '../app-config.js';
import {buildReflectServerContent} from '../compile.js';
import {ErrorWrapper} from '../error.js';
import {getScriptTemplate} from '../get-script-template.js';
import {inspectorConsoleClient} from './inspector-console-client.js';

/**
 * Returns a function that shuts down the dev server.
 */
export async function startDevServer(
  code: OutputFile,
  sourcemap: OutputFile,
  port: number,
  mode: 'production' | 'development',
  signal: AbortSignal,
): Promise<URL> {
  const appDir = path.dirname(code.path);
  const appConfigRoot = mustFindAppConfigRoot();
  const inspectorPort = await getPort({port: 9229});

  // Create a new Miniflare instance, starting a workerd server
  const mf = new Miniflare({
    port,
    modules: [
      {
        type: 'ESModule',
        path: path.join(appDir, 'worker.js'),
        contents: await getScriptTemplate(
          'dev',
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
        contents: await buildReflectServerContent(mode),
      },
    ],
    bindings: {
      ['REFLECT_AUTH_API_KEY']: nanoid(),
    },

    durableObjects: {roomDO: 'RoomDO', authDO: 'AuthDO', testDO: 'TestDO'},

    durableObjectsPersist: path.join(appConfigRoot, '.reflect', 'data'),

    // Use inspector/Chrome DevTools Protocol to forward console.log inside the
    // worker to console.log to the terminal.
    inspectorPort,

    compatibilityDate: '2023-05-18',
    compatibilityFlags: ['nodejs_compat'],
  });

  let url;
  try {
    url = await mf.ready;
  } catch (e) {
    // Errors from Miniflare initialization are more likely to represent a
    // problem with the customer's code or environment, rather than a problem
    // with our code.
    throw new ErrorWrapper(e, 'WARNING');
  }

  await inspectorConsoleClient(url, inspectorPort, signal);

  signal.addEventListener(
    'abort',
    () => {
      mf.dispose().catch(e => {
        console.error('Failed to shut down dev server', e);
      });
    },
    {once: true},
  );

  return url;
}
