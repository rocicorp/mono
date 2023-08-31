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

// LocalConfig contains the user-specified features of the App.
const localConfigSchema = v.object({
  server: v.string(),
  apps: v.undefined().optional(),
});

// AppInstance identifies an app that has been initialized on the Mirror Server.
const appInstanceSchema = v.object({
  appID: v.string(),
});

const appInstancesSchema = v.record(appInstanceSchema);

// InitializedAppConfig combines the LocalConfig with one or more initialized AppInstances.
const initializedAppConfigSchema = v.object({
  server: v.string(),
  apps: appInstancesSchema,
});

const configFileSchema = v.union(localConfigSchema, initializedAppConfigSchema);

export type AppInstance = v.Infer<typeof appInstanceSchema>;
export type LocalConfig = v.Infer<typeof localConfigSchema>;
export type InitializedAppConfig = v.Infer<typeof initializedAppConfigSchema>;
export type ConfigFile = v.Infer<typeof configFileSchema>;

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

let appConfigForTesting: ConfigFile | undefined;

export function setAppConfigForTesting(config: ConfigFile | undefined) {
  appConfigForTesting = config;
}

export function configFileExists(configDirPath: string): boolean {
  const configFilePath = getConfigFilePath(configDirPath);
  return fs.existsSync(configFilePath);
}

/**
 * Reads reflect.config.json in the "project root".
 */
export function readAppConfig(
  configDirPath?: string | undefined,
): ConfigFile | undefined {
  if (appConfigForTesting) {
    return appConfigForTesting;
  }
  const configFilePath = getConfigFilePath(configDirPath);
  if (fs.existsSync(configFilePath)) {
    const json = JSON.parse(fs.readFileSync(configFilePath, 'utf-8'));
    return v.parse(json, configFileSchema, 'passthrough');
  }

  return undefined;
}

export function mustReadAppConfig(
  configDirPath?: string | undefined,
): ConfigFile {
  const spec = readAppConfig(configDirPath);
  if (!spec) {
    throw new Error(
      `Could not find ${configFileName}. Please run \`reflect init\` to create one.`,
    );
  }
  return spec;
}

export async function ensureAppInitialized(
  instance = 'default',
): Promise<LocalConfig & AppInstance> {
  let config = mustReadAppConfig();
  if (config.apps?.[instance]) {
    return {
      server: config.server,
      ...config.apps?.[instance],
    };
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
  config = writeAppConfig({...config, apps: {[instance]: {appID}}});
  return {
    server: config.server,
    ...config.apps?.[instance],
  };
}

export function writeAppConfig<
  Config extends LocalConfig | InitializedAppConfig,
>(config: Config, configDirPath?: string | undefined): Config {
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
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore type error in jest?!?
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
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore type error in jest?!?
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
