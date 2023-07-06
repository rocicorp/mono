import jwtDecode from 'jwt-decode';
import fs, {mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import * as v from 'shared/src/valita.js';
import {parse} from 'shared/src/valita.js';
import {scriptName} from './create-cli-parser.js';
/**
 * The path to the config file that holds user authentication data,
 * relative to the user's home directory.
 */
export const USER_AUTH_CONFIG_FILE = 'config/default.json';

/**
 * The data that may be read from the `USER_CONFIG_FILE`.
 */

export const userAuthConfigSchema = v.object({
  idToken: v.string(),
  refreshToken: v.string(),
  expirationTime: v.number(),
});
export type UserAuthConfig = v.Infer<typeof userAuthConfigSchema>;

/**
 * Writes a a reflect config file (auth credentials) to disk,
 * and updates the user auth state with the new credentials.
 */

export function writeAuthConfigFile(config: UserAuthConfig) {
  const authConfigFilePath = path.join(
    getGlobalReflectConfigPath(),
    USER_AUTH_CONFIG_FILE,
  );
  mkdirSync(path.dirname(authConfigFilePath), {
    recursive: true,
  });
  writeFileSync(authConfigFilePath, JSON.stringify(config, null, 2), 'utf-8');
}

function isDirectory(configPath: string) {
  try {
    return fs.statSync(configPath).isDirectory();
  } catch {
    // ignore error
    return false;
  }
}

export function getGlobalReflectConfigPath() {
  const configPath = path.join(os.homedir(), '.reflect');
  if (!isDirectory(configPath)) {
    mkdirSync(configPath, {recursive: true});
  }
  return configPath;
}

let authConfigForTesting: UserAuthConfig | undefined;

export function setAuthConfigForTesting(config: UserAuthConfig | undefined) {
  authConfigForTesting = config;
}

//todo: make test
export function mustReadAuthConfigFile(): UserAuthConfig {
  if (authConfigForTesting) {
    return authConfigForTesting;
  }
  const authConfigFilePath = path.join(
    getGlobalReflectConfigPath(),
    USER_AUTH_CONFIG_FILE,
  );
  try {
    const rawData = readFileSync(authConfigFilePath, 'utf-8');
    const config: UserAuthConfig = JSON.parse(rawData);
    return parse(config, userAuthConfigSchema);
  } catch (err) {
    if (isFileNotFoundError(err)) {
      throw new Error(
        `No config file found. Please run \`${scriptName} login\` to authenticate.`,
      );
    }

    throw err;
  }
}

function isFileNotFoundError(err: unknown): boolean {
  return (
    err instanceof Error &&
    (err as unknown as {code?: unknown}).code === 'ENOENT'
  );
}

const tokenSchema = v.object({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  user_id: v.string(),
});

export function getUserIDFromConfig(config: UserAuthConfig) {
  // @ts-expect-error TS reports an error about the default export not being a
  // function but it clearly is.
  const token = jwtDecode(config.idToken);
  // Use passthrough to allow extra properties
  v.assert(token, tokenSchema, 'passthrough');
  return token.user_id;
}
