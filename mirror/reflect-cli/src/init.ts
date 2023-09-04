import {existsSync} from 'node:fs';
import {execSync} from 'node:child_process';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import color from 'picocolors';
import {copyTemplate} from './scaffold.js';
import {configFileExists} from './app-config.js';

export function initOptions(yargs: CommonYargsArgv) {
  return yargs;
}

type InitHandlerArgs = YargvToInterface<ReturnType<typeof initOptions>>;

export async function initHandler(_: InitHandlerArgs) {
  if (configFileExists('./')) {
    console.log(
      `Cannot initialize. There is already a ${color.white(
        'reflect.config.json',
      )} file present.`,
    );
    process.exit(1);
  }
  if (existsSync('reflect')) {
    console.log(
      `Cannot initialize. There is already a ${color.white(
        'reflect',
      )} folder present.`,
    );
    process.exit(1);
  }
  if (!existsSync('package.json')) {
    console.log(
      `No package.json. To create an example project, run:\nnpx @rocicorp/reflect create <name>`,
    );
    process.exit(1);
  }

  await copyTemplate('init', './');
  console.log(`Installing @rocicorp/reflect`);
  execSync(`npm add '@rocicorp/reflect'`, {
    stdio: ['ignore', 'inherit', 'inherit'],
  });

  console.log(color.green(`\nYou're all set! 🎉`));
  console.log(color.blue(`\nTo start the Reflect dev server:\n`));

  console.log(color.reset('npx @rocicorp/reflect dev'));
}
