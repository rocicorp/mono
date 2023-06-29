import fs, {mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import * as v from 'shared/src/valita.js';
import {parse} from 'shared/src/valita.js';
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
  writeFileSync(
    path.join(authConfigFilePath),
    JSON.stringify(config, null, 2),
    'utf-8',
  );
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

//todo: make test
export function mustReadAuthConfigFile(): UserAuthConfig {
  const authConfigFilePath = path.join(
    getGlobalReflectConfigPath(),
    USER_AUTH_CONFIG_FILE,
  );
  try {
    const rawData = readFileSync(authConfigFilePath, 'utf-8');
    const config: UserAuthConfig = JSON.parse(rawData);
    return parse(config, userAuthConfigSchema);
  } catch (error) {
    // If the file does not exist or it cannot be parsed, return an empty object
    console.warn(`Unable to read or parse auth config file: ${error}`);
    throw new Error(`Unable to read or parse auth config file: ${error}`);
  }
}
