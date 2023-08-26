import * as fs from 'node:fs';
import * as path from 'node:path';
import {pkgUpSync} from 'pkg-up';
import * as v from 'shared/src/valita.js';
import {authenticate} from './auth-config.js';
import {makeRequester} from './requester.js';
import {CreateRequest, create} from 'mirror-protocol/src/app.js';

// AppSpec contains the user-specified features of the App.
const appSpecSchema = v.object({
  id: v.string().optional(), // Initialized on first online action (e.g. publish).
  name: v.string(),
  server: v.string(),
});

// AppConfig extends AppSpec and includes the initialized app ID on the backend.
const appConfigSchema = v.object({
  id: v.string(),
  name: v.string(),
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
  if (spec.id) {
    // TODO(darick): Check that the name matches that of the app? Or somehow support renaming.
    return v.parse(spec, appConfigSchema, 'passthrough');
  }
  const {uid: userID} = await authenticate();

  const data: CreateRequest = {
    requester: makeRequester(userID),
    serverReleaseChannel: 'stable',
    name: spec.name,
  };

  const {appID} = await create(data);
  const config = {
    ...spec,
    id: appID,
  };
  writeAppConfig(config, configDirPath);
  return config;
}

export function writeAppConfig(
  config: AppSpec,
  configDirPath?: string | undefined,
) {
  const configFilePath = getConfigFilePath(configDirPath);
  fs.writeFileSync(configFilePath, JSON.stringify(config, null, 2), 'utf-8');
}
