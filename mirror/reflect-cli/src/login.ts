import {resolver} from '@rocicorp/resolver';
import assert from 'node:assert';
import http from 'node:http';
import type {Socket} from 'node:net';
import open from 'open';
import {sleep} from 'shared/src/sleep.js';
import {parse} from 'shared/src/valita.js';
import {
  authCredentialSchema,
  UserAuthConfig,
  writeAuthConfigFile as writeAuthConfigFileImpl,
} from './auth-config.js';
import {ErrorWithSeverity} from './error.js';
import {confirm} from './inquirer.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {getLogger} from './logger.js';

async function timeout(signal: AbortSignal) {
  await sleep(120_000, signal);
  throw new ErrorWithSeverity(
    'Login did not complete within 2 minutes',
    'WARNING',
  );
}

export async function loginHandler(
  yargs: YargvToInterface<CommonYargsArgv>,
  promptToOpenBrowser = true,
  openInBrowser = openInBrowserImpl,
  writeAuthConfigFile = writeAuthConfigFileImpl,
): Promise<void> {
  const PROD_DOMAIN = 'reflect.net';
  const SANDBOX_DOMAIN = 'sandbox.reflect.net';
  const BASE_URL = yargs.local
    ? 'http://localhost:3000'
    : yargs.stack === 'prod'
    ? `https://${PROD_DOMAIN}`
    : `https://${SANDBOX_DOMAIN}`;
  const urlToOpen = process.env.AUTH_URL || `${BASE_URL}/auth`;

  const loginResolver = resolver();
  const credentialReceiverServer = http.createServer((req, res) => {
    assert(req.url, "This request doesn't have a URL"); // This should never happen
    const reqUrl = new URL(req.url, `https://${req.headers.host}`);
    const {pathname, searchParams} = reqUrl;

    switch (pathname) {
      case '/oauth/callback': {
        const authCredential = searchParams.get('authCredential');
        try {
          if (!authCredential) {
            throw new Error(`Missing auth credential from the auth provider.`);
          }
          const authConfig: UserAuthConfig = {
            authCredential: parse(
              JSON.parse(authCredential),
              authCredentialSchema,
              'passthrough',
            ),
          };

          writeAuthConfigFile(yargs, authConfig);
        } catch (error) {
          res.end(() => {
            loginResolver.reject(
              new Error('Error saving credentials: ' + error),
            );
          });
          return;
        }
        res.writeHead(307, {
          // eslint-disable-next-line @typescript-eslint/naming-convention
          Location: `${BASE_URL}/reflect-auth-welcome`,
        });
        res.end(() => {
          loginResolver.resolve();
        });
        return;
      }
    }
  });

  // keeping track of connections so that when we call a server close it
  // does not hold the process from exiting until all kee-alive connections are closed
  const connections = new Set<Socket>();
  credentialReceiverServer.on('connection', (conn: Socket) => {
    connections.add(conn);
    conn.on('close', () => {
      connections.delete(conn);
    });
  });

  credentialReceiverServer.listen(8976);

  if (
    !promptToOpenBrowser ||
    (await confirm({
      message: 'Open login page in your default browser?',
      default: true,
    }))
  ) {
    await openInBrowser(urlToOpen);
  }
  getLogger().log(`Please login at: ${urlToOpen}`);
  const timeoutController = new AbortController();
  try {
    await Promise.race([
      timeout(timeoutController.signal),
      loginResolver.promise,
    ]);
  } finally {
    timeoutController.abort();
    credentialReceiverServer.close((closeErr?: Error) => {
      if (closeErr) {
        getLogger().warn('login credential server failed to close', closeErr);
      }
    });

    //destroying all sockets to close all keep-alive connections
    for (const socket of connections.values()) {
      socket.destroy();
    }
  }
}

/**
 * An extremely simple wrapper around the open command.
 * Specifically, it adds an 'error' event handler so that when this function
 * is called in environments where we can't open the browser (e.g. GitHub Codespaces,
 * StackBlitz, remote servers), it doesn't just crash the process.
 *
 * @param url the URL to point the browser at
 */
export default async function openInBrowserImpl(url: string): Promise<void> {
  const childProcess = await open(url);
  childProcess.on('error', () => {
    getLogger().warn('Failed to open');
  });
}
