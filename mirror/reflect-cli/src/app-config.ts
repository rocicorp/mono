import * as fs from 'node:fs';
import * as path from 'node:path';
import {pkgUpSync} from 'pkg-up';

export type AppConfig = {
  appID: string;
};

/**
 * Finds the root of the git repository.
 */
function findGitRoot(p = process.cwd()) {
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

function mustFindConfigRoot(): string {
  const configRoot = findConfigRoot();
  if (!configRoot) {
    throw new Error(
      'Could not find config root. Either a package.json or a .git directory is required.',
    );
  }
  return configRoot;
}

function mustFindConfigFilePath(): string {
  const configRoot = mustFindConfigRoot();
  return path.join(configRoot, configFileName);
}

const configFileName = 'reflect.config.json';

let appConfigForTesting: AppConfig | undefined;

export function setAppConfigForTesting(config: AppConfig | undefined) {
  appConfigForTesting = config;
}

/**
 * Reads reflect.config.json in the "project root".
 */
export function readAppConfig(): AppConfig | undefined {
  if (appConfigForTesting) {
    return appConfigForTesting;
  }
  const configFilePath = mustFindConfigFilePath();
  if (fs.existsSync(configFilePath)) {
    return JSON.parse(fs.readFileSync(configFilePath, 'utf-8'));
  }

  return undefined;
}

export function mustReadAppConfig(): AppConfig {
  const config = readAppConfig();
  if (!config) {
    throw new Error(
      `Could not find ${configFileName}. Please run \`reflect init\` to create one.`,
    );
  }
  return config;
}

export function writeAppConfig(config: AppConfig) {
  const configFilePath = mustFindConfigFilePath();
  console.log('Writing config to', configFilePath);
  fs.writeFileSync(configFilePath, JSON.stringify(config, null, 2), 'utf-8');
}
