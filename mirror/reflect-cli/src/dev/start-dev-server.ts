import type {LogLevel} from '@rocicorp/logger';
import type {OutputFile} from 'esbuild';
import getPort from 'get-port';
import {SERVER_VARIABLE_PREFIX} from 'mirror-schema/src/external/vars.js';
import {nanoid} from 'nanoid';
import * as path from 'node:path';
import {mustFindAppConfigRoot} from '../app-config.js';
import {buildReflectServerContent} from '../compile.js';
import {ErrorWrapper} from '../error.js';
import {getScriptTemplate} from '../get-script-template.js';
import {MiniflareWrapper} from './miniflare-wrapper.js';
import {listDevVars} from './vars.js';

/**
 * To shut down the dev server, abort the passed in signal.
 */
export async function startDevServer(
  code: OutputFile,
  sourcemap: OutputFile,
  port: number,
  mode: 'production' | 'development',
  logLevel: LogLevel,
  signal: AbortSignal,
): Promise<URL> {
  const appDir = path.dirname(code.path);
  const appConfigRoot = mustFindAppConfigRoot();
  const inspectorPort = await getPort({port: 9229});

  const devVars = listDevVars();
  const devBindings = Object.fromEntries(
    Object.entries(devVars).map(([key, value]) => [
      `${SERVER_VARIABLE_PREFIX}${key}`,
      value,
    ]),
  );

  // Create a new Miniflare instance, starting a workerd server
  const mf = new MiniflareWrapper({
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
      ['REFLECT_API_KEY']: nanoid(),
      ['LOG_LEVEL']: logLevel,
      ...devBindings,
    },

    durableObjects: {roomDO: 'RoomDO', authDO: 'AuthDO'},

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
