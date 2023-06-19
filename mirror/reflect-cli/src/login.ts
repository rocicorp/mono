import assert from 'node:assert';
import http from 'node:http';
import url from 'node:url';
import open from 'open';
import {writeAuthConfigFile} from './auth-config.js';

export async function loginHandler() {
  await login();
}

export async function login(): Promise<boolean> {
  //const urlToOpen = await 'https://auth.reflect.net';
  const urlToOpen = await 'http://localhost:3000';
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
    server = http.createServer((req, res) => {
      function finish(status: boolean, error?: Error) {
        clearTimeout(loginTimeoutHandle);
        server.close((closeErr?: Error) => {
          if (error || closeErr) {
            reject(error || closeErr);
          } else resolve(status);
        });
      }

      assert(req.url, "This request doesn't have a URL"); // This should never happen
      const {pathname, query} = url.parse(req.url, true);
      console.log(`pathname: ${pathname}`);
      switch (pathname) {
        case '/oauth/callback': {
          //log request headers post
          console.log(req.headers);
          // eslint-disable-next-line prefer-destructuring
          //get idToken from url parameter
          const {idToken, refreshToken, expiresIn} = query;
          if (!idToken || !refreshToken || !expiresIn) {
            // render an error page here
            res.end(() => {
              finish(false, new Error('No idToken or refreshToken provided'));
            });
            return;
          }
          if (
            idToken instanceof Array ||
            refreshToken instanceof Array ||
            expiresIn instanceof Array ||
            isNaN(parseInt(expiresIn))
          ) {
            res.end(() => {
              finish(
                false,
                new Error(
                  'Invalid idToken or refreshToken or expiresIn provided',
                ),
              );
            });
          } else {
            writeAuthConfigFile({
              idToken,
              refreshToken,
              expiresIn: parseInt(expiresIn),
            });
          }
          // todo: have a success page on auth-ui
          res.writeHead(307, {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            Location: 'http://localhost:3000/reflect-auth-welcome',
          });
          res.end(() => {
            finish(true);
          });
          console.log(`Successfully logged in.`);
          return;
        }
      }
    });

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
