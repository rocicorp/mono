import {doc, getDoc, getFirestore} from 'firebase/firestore';
import {createApp} from 'mirror-protocol/src/app.js';
import {ensureTeam} from 'mirror-protocol/src/team.js';
import {
  appNameIndexDataConverter,
  appNameIndexPath,
} from 'mirror-schema/src/external/team.js';
import * as fs from 'node:fs';
import * as path from 'node:path';
import {pkgUpSync} from 'pkg-up';

import * as v from 'shared/src/valita.js';
import {ErrorWrapper, UserError} from './error.js';
import type {AuthContext} from './handler.js';
import {logErrorAndExit} from './log-error-and-exit.js';
import {makeRequester} from './requester.js';
import {isValidAppName} from 'mirror-schema/src/external/app.js';
// { srcFile: destFile }
const templateFiles = v.record(v.string());

export const DEFAULT_FROM_REFLECT_CONFIG = '(from reflect.config.json)';
// AppInstance identifies an app that has been initialized on mirror via app-create.
const appInstanceSchema = v.object({
  appID: v.string(),
});

// AppInstances associates an instance name (e.g. "default", "dev", "prod" with an
// AppInstance. Currently, the cli is hardcoded to always use the "default" instance,
// but the schema supports tracking multiple instances.
const appInstancesSchema = v.record(appInstanceSchema);

// LocalConfig contains the user-specified features of the App.
const localConfigSchema = v.object({
  server: v.string(),
});

// AppConfig tracks the full state of the directory and App(s).
const appConfigSchema = v.object({
  server: v.string(),
  templates: templateFiles.optional(),
  apps: appInstancesSchema.optional(),
});

export type AppInstance = v.Infer<typeof appInstanceSchema>;
export type LocalConfig = v.Infer<typeof localConfigSchema>;
export type AppConfig = v.Infer<typeof appConfigSchema>;

/**
 * Finds the root of the git repository.
 */
function findGitRoot(p = process.cwd()): string | undefined {
  const gitDir = path.join(p, '.git');
  if (fs.existsSync(gitDir)) {
    return p;
  }
  if (p === path.sep || !fs.existsSync(p)) {
    return undefined;
  }
  const parent = path.join(p, '..');
  return findGitRoot(parent);
}

function findConfigRoot(): string | undefined {
  const pkg = pkgUpSync();
  if (pkg) {
    return path.dirname(pkg);
  }
  return findGitRoot();
}

export function mustFindAppConfigRoot(): string {
  const configRoot = findConfigRoot();
  if (!configRoot) {
    throw new Error(
      'Could not find config root. Either a package.json or a .git directory is required.',
    );
  }
  return configRoot;
}

function findConfigFilePath(): string | undefined {
  const configRoot = findConfigRoot();
  if (!configRoot) {
    return undefined;
  }
  return path.join(configRoot, configFileName);
}

function getConfigFilePath(configDirPath?: string | undefined) {
  return configDirPath
    ? path.join(configDirPath, configFileName)
    : findConfigFilePath();
}

const configFileName = 'reflect.config.json';

let appConfigForTesting: AppConfig | undefined;

export function setAppConfigForTesting(config: AppConfig | undefined) {
  appConfigForTesting = config;
}

export function getDefaultServerPath() {
  const config = readAppConfig();
  if (config?.server) {
    return DEFAULT_FROM_REFLECT_CONFIG;
  }
  return './reflect-server/index.ts';
}

export function getAppIDfromConfig(instance = 'default') {
  const config = readAppConfig();
  return config?.apps?.[instance]?.appID;
}

export function getDefaultApp() {
  const configAppId = getAppIDfromConfig();
  if (configAppId) {
    return DEFAULT_FROM_REFLECT_CONFIG;
  }
  return undefined;
}
/**
 * Reads reflect.config.json in the "project root".
 */
export function readAppConfig(
  configDirPath?: string | undefined,
): AppConfig | undefined {
  if (appConfigForTesting) {
    return appConfigForTesting;
  }
  const configFilePath = getConfigFilePath(configDirPath);
  if (!configFilePath) {
    return undefined;
  }
  if (fs.existsSync(configFilePath)) {
    try {
      const json = JSON.parse(fs.readFileSync(configFilePath, 'utf-8'));
      return v.parse(json, appConfigSchema, 'passthrough');
    } catch (e) {
      // reflect.config.json is sometimes modified by users. Classify parse/syntax
      // errors as WARNINGS so that we are only alerted if they happen at a higher threshold.
      throw new ErrorWrapper(e, 'WARNING');
    }
  }
  return undefined;
}

export function mustReadAppConfig(
  configDirPath?: string | undefined,
): AppConfig {
  const config = readAppConfig(configDirPath);
  if (!config) {
    logErrorAndExit(`Could not find ${configFileName}.`);
  }
  return config;
}

export async function ensureTeamID(authContext: AuthContext): Promise<string> {
  const {userID, additionalUserInfo} = authContext.user;
  const requester = makeRequester(userID);
  if (userID.startsWith('teams/')) {
    // API key authentication is already associated with a team.
    return userID.split('/')[1];
  }
  const defaultTeamName = additionalUserInfo?.username;
  if (!defaultTeamName) {
    throw new Error('Could not determine GitHub username from OAuth');
  }
  const ensured = await ensureTeam.call({
    requester,
    name: defaultTeamName,
  });
  return ensured.teamID;
}

async function getAppIDfromAppName(
  teamID: string,
  appName: string,
): Promise<string | undefined> {
  mustValidAppName(appName);
  const firestore = getFirestore();
  const nameEntry = await getDoc(
    doc(firestore, appNameIndexPath(teamID, appName)).withConverter(
      appNameIndexDataConverter,
    ),
  );
  return nameEntry.data()?.appID;
}

export async function getAppID(
  authContext: AuthContext,
  app: string,
  create = false,
): Promise<string> {
  if (app === DEFAULT_FROM_REFLECT_CONFIG) {
    const appID = getAppIDfromConfig();
    if (!appID) {
      logErrorAndExit('No appID found in reflect.config.json');
    }
    return appID;
  }
  // Otherwise it's a name.
  const teamID = await ensureTeamID(authContext);
  const appID = await getAppIDfromAppName(teamID, app); // From the index in Firestore
  if (appID) {
    return appID;
  }
  if (!create) {
    console.log(
      `The "${app}" app must first be published with "npx reflect publish"`,
    );
    process.exit(-1);
  }
  console.log(`Creating the "${app}" app ...`);
  const requester = makeRequester(authContext.user.userID);
  const resp = await createApp.call({
    requester,
    teamID,
    name: app,
    serverReleaseChannel: 'stable',
  });

  return resp.appID;
}

export function writeAppConfig(
  config: AppConfig,
  configDirPath?: string | undefined,
) {
  const configFilePath = getConfigFilePath(configDirPath);
  if (!configFilePath) {
    throw new Error('Could not find config file path');
  }
  fs.writeFileSync(configFilePath, JSON.stringify(config, null, 2), 'utf-8');
}

type TemplatePlaceholders = {
  appName: string;
  appHostname: string;
  reflectVersion: string;
};

export function writePackageJson(
  placeholders: Partial<{[Key in keyof TemplatePlaceholders]: string}>,
  dir = './',
  logConsole = true,
) {
  copyAndEditFile(
    dir,
    'package.json',
    'package.json',
    content => {
      for (const [key, value] of Object.entries(placeholders)) {
        content = content.replaceAll(`{{${key}}}`, value);
      }
      return content;
    },
    logConsole,
  );
}

function copyAndEditFile(
  dir: string,
  src: string,
  dst: string,
  edit: (content: string) => string,
  logConsole: boolean,
) {
  try {
    const srcPath = path.resolve(dir, src);
    const dstPath = path.resolve(dir, dst);
    const content = fs.readFileSync(srcPath, 'utf-8');
    const edited = edit(content);
    if (
      fs.existsSync(dstPath) &&
      fs.readFileSync(dstPath, 'utf-8') === edited
    ) {
      return;
    }
    fs.writeFileSync(dstPath, edited, 'utf-8');
    if (logConsole) {
      console.log(`Updated ${dst} from ${src}`);
    }
  } catch (e) {
    // In case the user has deleted the template source file, classify this as a
    // warning instead.
    throw new ErrorWrapper(e, 'WARNING');
  }
}

export function mustValidAppName(appName: string) {
  if (!isValidAppName(appName) && appName !== DEFAULT_FROM_REFLECT_CONFIG) {
    throw new UserError(
      `Invalid App Name "${appName}". Names must be lowercased alphanumeric, starting with a letter and not ending with a hyphen.`,
    );
  }
}
