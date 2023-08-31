import * as fs from 'node:fs';
import {readFile} from 'fs/promises';
import * as path from 'node:path';
import {pkgUpSync} from 'pkg-up';
import * as v from 'shared/src/valita.js';
import confirm from '@inquirer/confirm';
import input from '@inquirer/input';
import {authenticate} from './auth-config.js';
import {makeRequester} from './requester.js';
import {createApp} from 'mirror-protocol/src/app.js';
import {ensureTeam} from 'mirror-protocol/src/team.js';
import {
  appNameIndexPath,
  appNameIndexDataConverter,
} from 'mirror-schema/src/team.js';
import {
  appPath,
  appDataConverter,
  isValidAppName,
} from 'mirror-schema/src/app.js';
import {getFirestore} from './firebase.js';
import {getDefaultAppNameFromDir} from './lfg.js';
import {must} from 'shared/src/must.js';
import {writeTemplatedFilePlaceholders} from './scaffold.js';

// AppSpec contains the user-specified features of the App.
const appSpecSchema = v.object({
  appID: v.string().optional(), // Initialized on first online action (e.g. publish).
  server: v.string(),
});

// AppConfig extends AppSpec and includes the initialized App ID on the backend.
const appConfigSchema = v.object({
  appID: v.string(),
  server: v.string(),
});

export type AppSpec = v.Infer<typeof appSpecSchema>;
export type AppConfig = v.Infer<typeof appConfigSchema>;

/**
 * Finds the root of the git repository.
 */
function findGitRoot(p = process.cwd()): string | undefined {
  if (!fs.existsSync(p)) {
    return undefined;
  }

  const gitDir = path.join(p, '.git');
  if (fs.existsSync(gitDir)) {
    return p;
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

/**
 * Reads reflect.config.json in the "project root".
 */
export function readAppSpec(
  configDirPath?: string | undefined,
): AppSpec | undefined {
  if (appConfigForTesting) {
    return appConfigForTesting;
  }
  const configFilePath = getConfigFilePath(configDirPath);
  if (fs.existsSync(configFilePath)) {
    const json = JSON.parse(fs.readFileSync(configFilePath, 'utf-8'));
    return v.parse(json, appSpecSchema, 'passthrough');
  }

  return undefined;
}

export function mustReadAppSpec(configDirPath?: string | undefined): AppSpec {
  const spec = readAppSpec(configDirPath);
  if (!spec) {
    throw new Error(
      `Could not find ${configFileName}. Please run \`reflect init\` to create one.`,
    );
  }
  return spec;
}

export async function ensureAppConfig(
  configDirPath?: string | undefined,
): Promise<AppConfig> {
  const spec = mustReadAppSpec(configDirPath);
  if (spec.appID) {
    return v.parse(spec, appConfigSchema, 'passthrough');
  }
  const {uid: userID, additionalUserInfo} = await authenticate(false);
  const defaultTeamName = additionalUserInfo?.username;
  if (!defaultTeamName) {
    throw new Error('Could not determine github username from OAuth');
  }
  const requester = makeRequester(userID);
  const {teamID} = await ensureTeam({
    requester,
    name: defaultTeamName,
  });
  const app = await getNewAppNameOrExistingID(teamID);
  const appID =
    app.id !== undefined
      ? app.id
      : (
          await createApp({
            requester,
            teamID,
            name: app.name,
            serverReleaseChannel: 'stable',
          })
        ).appID;
  // Now that the App is created, fill in the TEAM-SUBDOMAIN placeholders in any scaffolding.
  const appDoc = await getFirestore()
    .doc(appPath(appID))
    .withConverter(appDataConverter)
    .get();
  const {teamSubdomain} = must(appDoc.data());
  writeTemplatedFilePlaceholders('./', {['<TEAM-SUBDOMAIN>']: teamSubdomain});
  return writeAppConfig({...spec, appID}, configDirPath);
}

export function writeAppConfig<Config extends AppSpec>(
  config: Config,
  configDirPath?: string | undefined,
): Config {
  const configFilePath = getConfigFilePath(configDirPath);
  fs.writeFileSync(configFilePath, JSON.stringify(config, null, 2), 'utf-8');
  return config;
}

async function getNewAppNameOrExistingID(
  teamID: string,
): Promise<{name: string; id?: undefined} | {id: string; name?: undefined}> {
  const firestore = getFirestore();
  const defaultAppName = await getDefaultAppName();
  if (isValidAppName(defaultAppName)) {
    const nameEntry = await firestore
      .doc(appNameIndexPath(teamID, defaultAppName))
      .withConverter(appNameIndexDataConverter)
      .get();
    if (!nameEntry.exists) {
      // Common case. The name in package.json is not taken. Create an app with it.
      return {name: defaultAppName};
    }
  }
  for (;;) {
    const name = await input({
      message: 'Name of your App:',
      default: defaultAppName,
      validate: isValidAppName,
    });
    const nameEntry = await firestore
      .doc(appNameIndexPath(teamID, name))
      .withConverter(appNameIndexDataConverter)
      .get();
    if (!nameEntry.exists) {
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
  const pkg = pkgUpSync();
  if (pkg) {
    const {name} = JSON.parse(await readFile(pkg, 'utf-8'));
    if (name) {
      return String(name);
    }
  }
  return getDefaultAppNameFromDir('./');
}
