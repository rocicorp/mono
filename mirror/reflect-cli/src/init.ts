import {existsSync} from 'node:fs';
import color from 'picocolors';
import {configFileExists} from './app-config.js';
import {execOrReportWarning} from './exec.js';
import {logErrorAndExit, noFormat} from './log-error-and-exit.js';
import {copyTemplate} from './scaffold.js';
import {findReflectVersion} from './version.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function initOptions(yargs: CommonYargsArgv) {
  return yargs;
}

type InitHandlerArgs = YargvToInterface<ReturnType<typeof initOptions>>;

export function initHandler(_: InitHandlerArgs) {
  if (configFileExists('./')) {
    logErrorAndExit(
      `Cannot initialize. There is already a ${color.white(
        'reflect.config.json',
      )} file present.`,
      noFormat,
    );
  }
  if (existsSync('reflect')) {
    logErrorAndExit(
      `Cannot initialize. There is already a ${color.white(
        'reflect',
      )} folder present.`,
      noFormat,
    );
  }
  if (!existsSync('package.json')) {
    logErrorAndExit(
      `No package.json. To create an example project, run:\nnpx @rocicorp/reflect create <name>`,
      noFormat,
    );
  }

  copyTemplate('init', './');
  console.log(`Installing @rocicorp/reflect`);

  const reflectVersion = findReflectVersion();
  execOrReportWarning(`npm add '@rocicorp/reflect@^${reflectVersion}'`, {
    stdio: ['ignore', 'inherit', 'inherit'],
  });

  console.log(color.green(`\nYou're all set! 🎉`));
  console.log(color.blue(`\nTo start the Reflect dev server:\n`));

  console.log(color.reset('npx @rocicorp/reflect dev'));
}
