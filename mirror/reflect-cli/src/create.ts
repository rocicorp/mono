import {execSync} from 'node:child_process';
import {mkdir} from 'node:fs/promises';
import color from 'picocolors';
import validateProjectName from 'validate-npm-package-name';
import {logErrorAndExit} from './log-error-and-exit.js';
import {scaffold} from './scaffold.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function createOptions(yargs: CommonYargsArgv) {
  return yargs.option('name', {
    describe: 'Name of the app',
    type: 'string',
    demandOption: true,
  });
}

type CreatedHandlerArgs = YargvToInterface<ReturnType<typeof createOptions>>;

export async function createHandler(createYargs: CreatedHandlerArgs) {
  const {name} = createYargs;

  const invalidPackageNameReason = isValidPackageName(name);
  if (invalidPackageNameReason) {
    logErrorAndExit(
      `Invalid project name: ${color.bgWhite(
        name,
      )} - (${invalidPackageNameReason})`,
    );
  }

  await mkdir(name, {recursive: true});
  scaffold(name, name);
  console.log(`Installing @rocicorp/reflect`);
  execSync(`npm add '@rocicorp/reflect'`, {
    cwd: name,
    stdio: ['ignore', 'inherit', 'inherit'],
  });

  console.log(color.green(`\nYou're all set! 🎉`));
  console.log(color.blue(`\nFirst, start the Reflect dev server:\n`));

  console.log(color.reset(`cd ${name}\nnpx @rocicorp/reflect dev`));
  console.log(color.blue('\nThen open a new terminal and run the UI:'));
  console.log(color.reset('\nVITE_WORKER_URL=ws://127.0.0.1:8080 npm run dev'));
}

function isValidPackageName(projectName: string): string | void {
  const nameValidation = validateProjectName(projectName);
  if (!nameValidation.validForNewPackages) {
    return [
      ...(nameValidation.errors || []),
      ...(nameValidation.warnings || []),
    ].join('\n');
  }
}
