import assert from 'node:assert';
import {mkdirSync, writeFileSync} from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import url from 'node:url';
import open from 'open';
import fs from 'node:fs';
import os from 'node:os';
import XDGAppPaths from 'xdg-app-paths';
export async function loginHandler() {
  await login();
}

export async function login(): Promise<boolean> {
  const urlToOpen = await 'https://auth.reflect.net';
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
      const {pathname} = url.parse(req.url, true);
      switch (pathname) {
        case '/oauth/callback': {
          let hasAuthCode = false;
          try {
            hasAuthCode = true;
            //hasAuthCode = isReturningFromAuthServer(query);
          } catch (err: unknown) {
            finish(false, err as Error);
            return;
          }

          if (!hasAuthCode) {
            // render an error page here
            finish(false, new Error('No auth code returned'));
            return;
          }
          //const exchange = await exchangeAuthCodeForAccessToken();
          writeAuthConfigFile({
            // eslint-disable-next-line @typescript-eslint/naming-convention
            oauth_token: 'a',
            // eslint-disable-next-line @typescript-eslint/naming-convention
            expiration_time: 'b',
            // eslint-disable-next-line @typescript-eslint/naming-convention
            refresh_token: 'c',
          });
          //   res.writeHead(307, {
          //     // eslint-disable-next-line @typescript-eslint/naming-convention
          //     Location:
          //       'https://welcome.developers.workers.dev/wrangler-oauth-consent-granted',
          //   });
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

/**
 * The path to the config file that holds user authentication data,
 * relative to the user's home directory.
 */
export const USER_AUTH_CONFIG_FILE = 'config/default.json';

/**
 * The data that may be read from the `USER_CONFIG_FILE`.
 */
export interface UserAuthConfig {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  oauth_token?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  refresh_token?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  expiration_time?: string;
}

/**
 * Writes a a wrangler config file (auth credentials) to disk,
 * and updates the user auth state with the new credentials.
 */

export function writeAuthConfigFile(config: UserAuthConfig) {
  const authConfigFilePath = path.join(
    getGlobalWranglerConfigPath(),
    USER_AUTH_CONFIG_FILE,
  );
  mkdirSync(path.dirname(authConfigFilePath), {
    recursive: true,
  });
  writeFileSync(
    path.join(authConfigFilePath),
    JSON.stringify(config, null, 2),
    {encoding: 'utf-8'},
  );
}

function isDirectory(configPath: string) {
  try {
    return fs.statSync(configPath).isDirectory();
  } catch (error) {
    // ignore error
    return false;
  }
}

export function getGlobalWranglerConfigPath() {
  //TODO: We should implement a custom path --global-config and/or the WRANGLER_HOME type environment variable
  const configDir = XDGAppPaths.default({suffix: '.reflect'}).config(); // New XDG compliant config path
  const legacyConfigDir = path.join(os.homedir(), '.reflect'); // Legacy config in user's home directory

  // Check for the .wrangler directory in root if it is not there then use the XDG compliant path.
  if (isDirectory(legacyConfigDir)) {
    return legacyConfigDir;
  }
  return configDir;
}
