import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import color from 'picocolors';
import {initApp, isValidPackageName} from './lfg.js';
import {mkdir} from 'node:fs/promises';

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

  await mkdir(name, {recursive: true});
  await initApp(createYargs, name);
}
