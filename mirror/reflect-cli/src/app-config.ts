import {doc, getDoc, getFirestore} from 'firebase/firestore';
import {readFile} from 'fs/promises';
import {createApp} from 'mirror-protocol/src/app.js';
import {ensureTeam} from 'mirror-protocol/src/team.js';
import {isValidAppName} from 'mirror-schema/src/external/app.js';
import {
  appNameIndexDataConverter,
  appNameIndexPath,
  sanitizeForSubdomain,
} from 'mirror-schema/src/external/team.js';
import * as fs from 'node:fs';
import * as path from 'node:path';
import {basename, resolve} from 'node:path';
import {pkgUp, pkgUpSync} from 'pkg-up';
import {must} from 'shared/src/must.js';
import {randInt} from 'shared/src/rand.js';
import * as v from 'shared/src/valita.js';
import {ErrorWrapper} from './error.js';
import type {AuthContext} from './handler.js';
import {confirm, input} from './inquirer.js';
import {logErrorAndExit} from './log-error-and-exit.js';
import {makeRequester} from './requester.js';

// { srcFile: destFile }
const templateFiles = v.record(v.string());

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

function mustFindConfigFilePath(): string {
  const configRoot = mustFindAppConfigRoot();
  return path.join(configRoot, configFileName);
}

function getConfigFilePath(configDirPath?: string | undefined) {
  return configDirPath
    ? path.join(configDirPath, configFileName)
    : mustFindConfigFilePath();
}

const configFileName = 'reflect.config.json';

let appConfigForTesting: AppConfig | undefined;

export function setAppConfigForTesting(config: AppConfig | undefined) {
  appConfigForTesting = config;
}

export function configFileExists(configDirPath: string): boolean {
  const configFilePath = getConfigFilePath(configDirPath);
  return fs.existsSync(configFilePath);
}

export function getDefaultServerPath() {
  const config = readAppConfig();
  return config?.server;
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

export async function ensureAppInstantiated(
  authContext: AuthContext,
  instance = 'default',
): Promise<LocalConfig & AppInstance> {
  const config = mustReadAppConfig();
  if (config.apps?.[instance]) {
    return {
      ...config,
      ...config.apps?.[instance],
    };
  }
  const {userID, additionalUserInfo} = authContext.user;
  const requester = makeRequester(userID);
  let teamID: string;
  if (userID.startsWith('team/')) {
    // API key authentication is already associated with a team.
    teamID = userID.split('/')[1];
  } else {
    const defaultTeamName = additionalUserInfo?.username;
    if (!defaultTeamName) {
      throw new Error('Could not determine GitHub username from OAuth');
    }
    const ensured = await ensureTeam.call({
      requester,
      name: defaultTeamName,
    });
    teamID = ensured.teamID;
  }
  const app = await getNewAppNameOrExistingID(teamID);
  const appID =
    app.id !== undefined
      ? app.id
      : (
          await createApp.call({
            requester,
            teamID,
            name: app.name,
            serverReleaseChannel: 'stable',
          })
        ).appID;
  writeAppConfig({...config, apps: {[instance]: {appID}}});
  return {...config, appID};
}

export function writeAppConfig(
  config: AppConfig,
  configDirPath?: string | undefined,
) {
  const configFilePath = getConfigFilePath(configDirPath);
  fs.writeFileSync(configFilePath, JSON.stringify(config, null, 2), 'utf-8');
}

async function getNewAppNameOrExistingID(
  teamID: string,
): Promise<{name: string; id?: undefined} | {id: string; name?: undefined}> {
  const firestore = getFirestore();
  const defaultAppName = await getDefaultAppName();
  if (isValidAppName(defaultAppName)) {
    const nameEntry = await getDoc(
      doc(firestore, appNameIndexPath(teamID, defaultAppName)).withConverter(
        appNameIndexDataConverter,
      ),
    );
    if (!nameEntry.exists()) {
      // Common case. The name in package.json is not taken. Create an app with it.
      return {name: defaultAppName};
    }
  }
  for (let appNameSuffix = ''; ; appNameSuffix = `-${randInt(1000, 9999)}`) {
    const name = await input({
      message: 'Name of your App:',
      default: `${defaultAppName}${appNameSuffix}`,
      validate: isValidAppName,
    });
    const nameEntry = await getDoc(
      doc(firestore, appNameIndexPath(teamID, name)).withConverter(
        appNameIndexDataConverter,
      ),
    );
    if (!nameEntry.exists()) {
      return {name};
    }
    const {appID: id} = must(nameEntry.data());
    if (
      await confirm({
        message: `There is an existing App named "${name}". Do you want to use it?`,
        default: false,
      })
    ) {
      return {id};
    }
  }
}

async function getDefaultAppName(): Promise<string> {
  const pkg = await pkgUp();
  if (pkg) {
    const {name} = JSON.parse(await readFile(pkg, 'utf-8'));
    if (name) {
      return String(name);
    }
  }
  return getDefaultAppNameFromDir('./');
}

function getDefaultAppNameFromDir(dir: string): string {
  const dirname = basename(resolve(dir));
  return sanitizeForSubdomain(dirname);
}

type TemplatePlaceholders = {
  appName: string;
  appHostname: string;
  reflectVersion: string;
};

export function writeTemplatedFilePlaceholders(
  placeholders: Partial<{[Key in keyof TemplatePlaceholders]: string}>,
  dir = './',
  logConsole = true,
) {
  const appConfig = mustReadAppConfig(dir);
  Object.entries(appConfig.templates ?? {}).forEach(([src, dst]) => {
    copyAndEditFile(
      dir,
      src,
      dst,
      content => {
        for (const [key, value] of Object.entries(placeholders)) {
          content = content.replaceAll(`{{${key}}}`, value);
        }
        return content;
      },
      logConsole,
    );
  });
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
