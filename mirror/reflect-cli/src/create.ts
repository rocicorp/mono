import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import color from 'picocolors';

import validateProjectName from 'validate-npm-package-name';
import {scaffoldHandler} from './scaffold.js';
// import {initHandler} from './init.js';
import {publishHandler} from './publish.js';

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
    console.log(
      color.red(
        `Invalid project name: ${color.bgWhite(
          name,
        )} - (${invalidPackageNameReason})`,
      ),
    );
    process.exit(1);
  }
  scaffoldHandler(createYargs);
  // await initHandler({
  //   ...createYargs,
  //   name: undefined,
  //   channel: 'stable',
  //   new: true,
  // });
  await publishHandler({
    ...createYargs,
    script: `${name}/src/worker/index.ts`,
  });
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
