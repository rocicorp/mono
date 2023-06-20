import fs, {readFileSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {mkdirSync, writeFileSync} from 'node:fs';

/**
 * The path to the config file that holds user authentication data,
 * relative to the user's home directory.
 */
export const USER_AUTH_CONFIG_FILE = 'config/default.json';

/**
 * The data that may be read from the `USER_CONFIG_FILE`.
 */
export interface UserAuthConfig {
  idToken?: string;
  refreshToken?: string;
  expiresIn?: number;
}

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

export function getGlobalReflectConfigPath() {
  const legacyConfigDir = path.join(os.homedir(), '.reflect'); // Legacy config in user's home directory
  if (!isDirectory(legacyConfigDir)) {
    //make the directory if it doesn't exist
    mkdirSync(legacyConfigDir, {recursive: true});
  }
  return legacyConfigDir;
}

//todo: make test
export function readAuthConfigFile(): UserAuthConfig | undefined {
  const authConfigFilePath = path.join(
    getGlobalReflectConfigPath(),
    USER_AUTH_CONFIG_FILE,
  );
  try {
    const rawData = readFileSync(authConfigFilePath, 'utf-8');
    const config: UserAuthConfig = JSON.parse(rawData);
    return config;
  } catch (error) {
    // If the file does not exist or it cannot be parsed, return an empty object
    console.warn(`Unable to read or parse auth config file: ${error}`);
    return undefined;
  }
}
