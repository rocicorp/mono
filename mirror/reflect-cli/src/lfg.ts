import {opendir} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import {basename, isAbsolute, resolve} from 'node:path';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import confirm from '@inquirer/confirm';
import input from '@inquirer/input';
import color from 'picocolors';
import {isValidAppName} from 'mirror-schema/src/app.js';
import {sanitizeForSubdomain} from 'mirror-schema/src/team.js';
import validateProjectName from 'validate-npm-package-name';
import {scaffold} from './scaffold.js';
import {configFileExists, writeAppConfig} from './app-config.js';

export function lfgOptions(yargs: CommonYargsArgv) {
  return yargs;
}

type LfgHandlerArgs = YargvToInterface<ReturnType<typeof lfgOptions>>;

export async function lfgHandler(yargs: LfgHandlerArgs) {
  await initApp(yargs, './');
}

export async function initApp(_: LfgHandlerArgs, dir: string) {
  if (configFileExists(dir)) {
    console.log(
      `Cannot initialize. There is already a ${color.white(
        'reflect.config.json',
      )} file present.`,
    );
    process.exit(1);
  }
  if (await canScaffold(dir)) {
    const name = await getAppName(dir);
    await scaffold(name, dir);
  } else {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore type error in jest?!?
    const server = await input({
      message: `Enter the path to the server entry point (e.g. ${color.white(
        'src/reflect/index.ts',
      )}):`,
      validate: validateEntryPoint(dir),
    });
    writeAppConfig({server}, dir);
  }

  console.log(color.green(`\nYou're all set! 🎉`));
  console.log(color.blue(`\nFirst, start the Reflect dev server:\n`));

  const STARTUP = '\nnpm install\nnpx reflect dev';
  console.log(color.reset((dir === './' ? '' : `cd ${dir}`) + STARTUP));
  console.log(color.blue('\nThen open a new terminal and run the UI:'));
  console.log(color.reset('\nVITE_WORKER_URL=ws://127.0.0.1:8080 npm run dev'));
}

async function canScaffold(dirPath: string): Promise<boolean> {
  const dir = await opendir(dirPath);
  for await (const _ of dir) {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore type error in jest?!?
    return confirm({
      message:
        'Current directory is not empty. Overwrite files with new project?',
      default: false,
    });
  }
  return true;
}

export function getDefaultAppNameFromDir(dir: string): string {
  const dirname = basename(resolve(dir));
  return sanitizeForSubdomain(dirname);
}

function getAppName(dir: string): Promise<string> {
  const defaultAppName = getDefaultAppNameFromDir(dir);
  if (validateAppName(defaultAppName) === true) {
    return Promise.resolve(defaultAppName);
  }
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore type error in jest?!?
  return input({
    message: 'Name of your App:',
    default: defaultAppName,
    validate: validateAppName,
  });
}

function validateAppName(name: string): string | boolean {
  if (!isValidAppName(name)) {
    return 'Names must start with a letter and use lowercased alphanumeric characters and hyphens.';
  }
  // This should never happen because isValidAppName is a subset of valid package names,
  // but just to be precise, we check this too.
  const invalidPackageNameReason = isValidPackageName(name);
  if (invalidPackageNameReason) {
    return invalidPackageNameReason;
  }
  return true;
}

export function isValidPackageName(projectName: string): string | void {
  const nameValidation = validateProjectName(projectName);
  if (!nameValidation.validForNewPackages) {
    return [
      ...(nameValidation.errors || []),
      ...(nameValidation.warnings || []),
    ].join('\n');
  }
}

function validateEntryPoint(dir: string) {
  return (path: string) => {
    if (isAbsolute(path)) {
      return 'Please specify a path relative to the project root.';
    }
    if (!existsSync(resolve(dir, path))) {
      return 'Please specify a valid file.';
    }
    return true;
  };
}
