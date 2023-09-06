import fs, {mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import * as v from 'shared/src/valita.js';
import color from 'picocolors';
import {parse} from 'shared/src/valita.js';
import {scriptName} from './create-cli-parser.js';
import {
  AuthCredential,
  EmailAuthCredential,
  getAdditionalUserInfo,
  getAuth,
  OAuthCredential,
  PhoneAuthCredential,
  SignInMethod,
  signInWithCredential,
  AdditionalUserInfo,
} from 'firebase/auth';
import {loginHandler} from './login.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

/**
 * The path to the config file that holds user authentication data,
 * relative to the user's home directory.
 */
export const USER_AUTH_CONFIG_FILE = 'config/default.json';

// https://firebase.google.com/docs/reference/js/auth.authcredential
export const authCredentialSchema = v.object({
  providerId: v.string(),
  signInMethod: v.string(),
});
export type JSONAuthCredential = v.Infer<typeof authCredentialSchema>;

/**
 * The data that may be read from the `USER_CONFIG_FILE`.
 */

export const userAuthConfigSchema = v.object({
  authCredential: authCredentialSchema,
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
function mustReadAuthConfigFile(authConfigFilePath: string): UserAuthConfig {
  try {
    const rawData = readFileSync(authConfigFilePath, 'utf-8');
    const config: UserAuthConfig = JSON.parse(rawData);
    return parse(config, userAuthConfigSchema, 'passthrough');
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

type AuthenticatedUser = {
  userID: string;
  getIdToken: (forceRefresh?: boolean | undefined) => Promise<string>;
  additionalUserInfo: AdditionalUserInfo | null;
};

export async function authenticate(
  yargs: YargvToInterface<CommonYargsArgv>,
  output = true,
): Promise<AuthenticatedUser> {
  if (authConfigForTesting) {
    return {
      userID: yargs.runAs ?? 'fake-uid',
      additionalUserInfo: null,
    } as unknown as AuthenticatedUser;
  }
  const authConfigFilePath = path.join(
    getGlobalReflectConfigPath(),
    USER_AUTH_CONFIG_FILE,
  );
  if (fs.statSync(authConfigFilePath, {throwIfNoEntry: false}) === undefined) {
    console.info('Login required');
    await loginHandler(yargs);
  }
  const config = mustReadAuthConfigFile(authConfigFilePath);
  const authCredential = parseAuthCredential(config.authCredential);
  if (!authCredential) {
    throw new Error(
      `Invalid credentials. Please run \`${scriptName} login\` again.`,
    );
  }
  const userCredentials = await signInWithCredential(getAuth(), authCredential);
  const additionalUserInfo = getAdditionalUserInfo(userCredentials);
  if (output) {
    console.info(`Logged in as ${userCredentials.user.email}`);
  }
  if (yargs.runAs) {
    console.info(color.yellow(`Running as ${yargs.runAs}`));
  }
  return {
    userID: yargs.runAs ?? userCredentials.user.uid,
    getIdToken: forceRefresh => userCredentials.user.getIdToken(forceRefresh),
    additionalUserInfo,
  };
}

function parseAuthCredential(json: JSONAuthCredential): AuthCredential | null {
  switch (json.signInMethod) {
    case SignInMethod.GITHUB:
    case SignInMethod.GOOGLE:
    case SignInMethod.TWITTER:
    case SignInMethod.FACEBOOK:
      return OAuthCredential.fromJSON(json);
    case SignInMethod.EMAIL_PASSWORD:
    case SignInMethod.EMAIL_LINK:
      return EmailAuthCredential.fromJSON(json);
    case SignInMethod.PHONE:
      return PhoneAuthCredential.fromJSON(json);
  }
  throw new Error('Invalid auth credentials. Please login again.');
}
