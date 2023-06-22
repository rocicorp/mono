import assert from 'node:assert';
import http from 'node:http';
import open from 'open';
import {
  UserAuthConfig,
  userAuthConfigSchema,
  writeAuthConfigFile,
} from './auth-config.js';
import {parse} from 'shared/valita.js';
import {sleep} from 'shared/sleep.js';
import {resolver} from '@rocicorp/resolver';
import type {Socket} from 'node:net';

async function timeout(signal: AbortSignal) {
  await sleep(120_000, signal);
  throw new Error('Login did not complete within 2 minutes');
}

export async function loginHandler(): Promise<void> {
  const urlToOpen = process.env.AUTH_URL || 'https://auth.reflect.net';
  const loginResolver = resolver<void>();
  const server = http.createServer((req, res) => {
    assert(req.url, "This request doesn't have a URL"); // This should never happen
    const reqUrl = new URL(req.url, `https://${req.headers.host}`);
    const {pathname, searchParams} = reqUrl;

    switch (pathname) {
      case '/oauth/callback': {
        const idToken = searchParams.get('idToken');
        const refreshToken = searchParams.get('refreshToken');
        const expirationTime = searchParams.get('expirationTime');

        try {
          if (!idToken || !refreshToken || !expirationTime) {
            throw new Error(
              'Missing idToken, refreshToken, or expiresIn from the auth provider.',
            );
          }

          const authConfig: UserAuthConfig = {
            idToken,
            refreshToken,
            expirationTime: parseInt(expirationTime),
          };
          parse(authConfig, userAuthConfigSchema);
          writeAuthConfigFile(authConfig);
        } catch (error) {
          res.end(() => {
            loginResolver.reject(
              new Error(
                'Invalid idToken, refreshToken, or expiresIn from the auth provider.',
              ),
            );
          });
          return;
        }
        res.writeHead(307, {
          // eslint-disable-next-line @typescript-eslint/naming-convention
          Location: `https://auth.reflect.net/reflect-auth-welcome`,
        });
        res.end(() => {
          loginResolver.resolve();
        });
        console.log('Successfully logged in.');
        return;
      }
    }
  });

  // keeping track of connections so that when we call a server close it
  // does not hold the process from exiting until all kee-alive connections are closed
  const connections = new Set<Socket>();
  server.on('connection', (conn: Socket) => {
    connections.add(conn);
    conn.on('close', () => {
      connections.delete(conn);
    });
  });

  server.listen(8976);

  console.log(`Opening a link in your default browser: ${urlToOpen}`);
  await openInBrowser(urlToOpen);
  const timeoutController = new AbortController();
  try {
    await Promise.race([
      timeout(timeoutController.signal),
      loginResolver.promise,
    ]);
  } finally {
    timeoutController.abort();
    server.close((closeErr?: Error) => {
      if (closeErr) {
        console.warn('login credential server failed to close', closeErr);
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
export default async function openInBrowser(url: string): Promise<void> {
  const childProcess = await open(url);
  childProcess.on('error', () => {
    console.warn('Failed to open');
  });
}
