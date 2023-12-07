import {
  AdditionalUserInfo,
  AuthCredential,
  EmailAuthCredential,
  OAuthCredential,
  PhoneAuthCredential,
  SignInMethod,
  UserCredential,
  getAdditionalUserInfo,
  getAuth,
  signInWithCredential,
  signInWithCustomToken,
} from 'firebase/auth';
import {createToken} from 'mirror-protocol/src/token.js';
import fs, {mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import color from 'picocolors';
import * as v from 'shared/src/valita.js';
import {parse} from 'shared/src/valita.js';
import {scriptName} from './create-cli-parser.js';
import {loginHandler} from './login.js';

import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

function getUserAuthConfigFile(
  yargs: YargvToInterface<CommonYargsArgv>,
): string {
  const {stack} = yargs;
  const basename = stack === 'prod' ? 'default' : stack;
  return path.join(getGlobalReflectConfigPath(), `config/${basename}.json`);
}

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

export function writeAuthConfigFile(
  yargs: YargvToInterface<CommonYargsArgv>,
  config: UserAuthConfig,
) {
  const authConfigFilePath = getUserAuthConfigFile(yargs);
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

export function getAuthentication(yargs: YargvToInterface<CommonYargsArgv>) {
  return authenticateImpl(yargs, false, false);
}

/** Prompts user to login if not authenticated. */
export function authenticate(
  yargs: YargvToInterface<CommonYargsArgv>,
  output = true,
) {
  return authenticateImpl(yargs, output, true);
}

async function authenticateImpl(
  yargs: YargvToInterface<CommonYargsArgv>,
  output = true,
  promptLogin = true,
): Promise<AuthenticatedUser> {
  const {runAs, authKeyFromEnv} = yargs;
  if (authConfigForTesting) {
    return {
      userID: runAs ?? 'fake-uid',
      additionalUserInfo: null,
    } as unknown as AuthenticatedUser;
  }
  let userCredentials: UserCredential;

  if (authKeyFromEnv) {
    const key = process.env[authKeyFromEnv];
    if (!key) {
      console.error(
        `${color.red(
          color.bold('Error'),
        )}: No key found in ${authKeyFromEnv} env variable`,
      );
      process.exit(-1);
    }
    const resp = await createToken({key});
    userCredentials = await signInWithCustomToken(getAuth(), resp.token);
  } else {
    const authConfigFilePath = getUserAuthConfigFile(yargs);
    if (
      fs.statSync(authConfigFilePath, {throwIfNoEntry: false}) === undefined
    ) {
      if (!promptLogin) {
        throw new Error(`No auth config file found.`);
      }
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
    userCredentials = await signInWithCredential(getAuth(), authCredential);
  }
  const additionalUserInfo = getAdditionalUserInfo(userCredentials);
  const {
    user: {email, uid},
  } = userCredentials;

  if (output) {
    if (email) {
      console.info(`Logged in as ${email}`);
    } else {
      console.info(
        // For UIDs such as "apps/ln3ddtrj/keys/abc-key", just show "abc-key".
        `Authenticated with ${uid.substring(uid.lastIndexOf('/') + 1)}`,
      );
    }
  }
  if (runAs) {
    console.info(color.yellow(`Running as ${runAs}`));
  }
  return {
    userID: runAs ?? uid,
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
