import fs, {mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import * as v from 'shared/src/valita.js';
import {parse} from 'shared/src/valita.js';
import {scriptName} from './create-cli-parser.js';
import {getAuth, signInWithCustomToken, type User} from 'firebase/auth';

/**
 * The path to the config file that holds user authentication data,
 * relative to the user's home directory.
 */
export const USER_AUTH_CONFIG_FILE = 'config/default.json';

/**
 * The data that may be read from the `USER_CONFIG_FILE`.
 */

export const userAuthConfigSchema = v.object({
  customToken: v.string(),
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
function mustReadAuthConfigFile(): UserAuthConfig {
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

export async function authenticate(): Promise<User> {
  if (authConfigForTesting) {
    return {uid: 'fake-uid'} as unknown as User;
  }
  const config = mustReadAuthConfigFile();
  const credentials = await signInWithCustomToken(
    getAuth(),
    config.customToken,
  );
  return credentials.user;
}
