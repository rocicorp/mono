import {opendir} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import {basename, isAbsolute, resolve} from 'node:path';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import confirm from '@inquirer/confirm';
import input from '@inquirer/input';
import color from 'picocolors';
import {isValidAppName, appNameIndexPath} from 'mirror-schema/src/app.js';
import validateProjectName from 'validate-npm-package-name';
import {getFirestore} from './firebase.js';
import {scaffold} from './scaffold.js';
import {writeAppConfig} from './app-config.js';
import {publishHandler} from './publish.js';

export function lfgOptions(yargs: CommonYargsArgv) {
  return yargs;
}

type LfgHandlerArgs = YargvToInterface<ReturnType<typeof lfgOptions>>;

export async function lfgHandler(yargs: LfgHandlerArgs) {
  await initApp(yargs, './');
}

export async function initApp(yargs: LfgHandlerArgs, dir: string) {
  const name = await getAppName(dir);
  if (await canScaffold(dir)) {
    scaffold(name, dir);
  } else {
    const server = await input({
      message:
        'Enter the path to the server entry point (e.g. src/reflect/index.ts):',
      validate: validateEntryPoint(dir),
    });
    writeAppConfig({name, server}, dir);
  }
  if (
    await confirm({
      message: `Publish to https://${name}.reflect-server.net/ ?`,
      default: true,
    })
  ) {
    await publishHandler(yargs, dir);
  }

  console.log('');
  console.log(color.green(`You're all set up! 🎉`));
  console.log(color.blue(`start-up your reflect app:\n`));

  const STARTUP = 'npm install && npm run dev\n';
  console.log(color.white((dir === './' ? '' : `cd ${name} && `) + STARTUP));
}

async function canScaffold(dirPath: string): Promise<boolean> {
  const dir = await opendir(dirPath);
  for await (const _ of dir) {
    return await confirm({
      message:
        'Current directory is not empty. Overwrite files with new project?',
      default: false,
    });
  }
  return true;
}

async function getAppName(dir: string): Promise<string> {
  const dirname = basename(resolve(dir));
  const defaultName = dirname
    .toLocaleLowerCase()
    .replaceAll(/^-*/g, '')
    .replaceAll(/[^a-z0-9\-]/g, '');
  return await input({
    message: 'Name of your App:',
    default: defaultName,
    validate: validateAppName,
  });
}

async function validateAppName(name: string): Promise<string | boolean> {
  if (!isValidAppName(name)) {
    return 'Names must start with a letter and use lowercased alphanumeric characters and hyphens.';
  }
  // This should never happen because isValidAppName is a subset of valid package names,
  // but just to be precise, we check this too.
  const invalidPackageNameReason = isValidPackageName(name);
  if (invalidPackageNameReason) {
    return invalidPackageNameReason;
  }
  const firestore = getFirestore();
  const app = await firestore.doc(appNameIndexPath(name)).get();
  if (app.exists) {
    return 'Looks like that name is already taken. Please choose another.';
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
  return async (path: string) => {
    if (isAbsolute(path)) {
      return 'Please specify a path relative to the project root.';
    }
    if (!existsSync(resolve(dir, path))) {
      return 'Please specify a valid file.';
    }
    return true;
  };
}
