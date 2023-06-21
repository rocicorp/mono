import assert from 'node:assert';
import http from 'node:http';
import open from 'open';
import {
  UserAuthConfig,
  userAuthConfigSchema,
  writeAuthConfigFile,
} from './auth-config.js';
import {parse} from 'shared/valita.js';

export async function loginHandler(): Promise<boolean> {
  const urlToOpen = process.env.AUTH_URL || 'https://auth.reflect.net';
  let server: http.Server;
  let loginTimeoutHandle: NodeJS.Timeout;
  const timerPromise = new Promise<boolean>(resolve => {
    loginTimeoutHandle = setTimeout(() => {
      console.error(
        'Timed out waiting for authorization code, please try again.',
      );
      server.close();

      clearTimeout(loginTimeoutHandle);
      resolve(false);
    }, 120000); // wait for 120 seconds for the user to authorize
  });

  const loginPromise = new Promise<boolean>((resolve, reject) => {
    const server = http.createServer((req, res) => {
      function finish(status: boolean, error?: Error) {
        clearTimeout(loginTimeoutHandle);
        server.close((closeErr?: Error) => {
          if (error || closeErr) {
            reject(error || closeErr);
          } else {
            resolve(status);
          }
        });
      }

      assert(req.url, "This request doesn't have a URL"); // This should never happen
      const reqUrl = new URL(req.url);
      const {pathname, searchParams} = reqUrl;
      console.log(`pathname: ${pathname}`);

      switch (pathname) {
        case '/oauth/callback': {
          const idToken = searchParams.get('idToken');
          const refreshToken = searchParams.get('refreshToken');
          const expiresIn = searchParams.get('expiresIn');

          try {
            if (!idToken || !refreshToken || !expiresIn) {
              throw new Error(
                'Invalid idToken, refreshToken, or expiresIn from the auth provider.',
              );
            }

            const authConfig: UserAuthConfig = {
              idToken,
              refreshToken,
              expiresIn: parseInt(expiresIn),
            };

            parse(authConfig, userAuthConfigSchema);
            writeAuthConfigFile(authConfig);
          } catch (error) {
            res.end(() => {
              finish(
                false,
                new Error(
                  'Invalid idToken, refreshToken, or expiresIn from the auth provider.',
                ),
              );
            });
            return;
          }

          res.end(() => {
            finish(true);
          });
          console.log('Successfully logged in.');
          return;
        }
      }
    });

    // Todo: Avoid hardcoding the port number
    server.listen(8976);
  });
  console.log(`Opening a link in your default browser: ${urlToOpen}`);
  await openInBrowser(urlToOpen);

  return Promise.race([timerPromise, loginPromise]);
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
