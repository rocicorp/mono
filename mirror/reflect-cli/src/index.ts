import {hideBin} from 'yargs/helpers';
import {authenticate} from './auth-config.js';
import {
  CommandLineArgsError,
  createCLIParserBase,
} from './create-cli-parser.js';
import {createHandler, createOptions} from './create.js';
import {deleteHandler, deleteOptions} from './delete.js';
import {devHandler, devOptions} from './dev.js';
import {handleWith} from './handler.js';
import {initHandler, initOptions} from './init.js';
import {loginHandler} from './login.js';
import {publishHandler, publishOptions} from './publish.js';
import {statusHandler} from './status.js';
import {tailHandler, tailOptions} from './tail/index.js';
import {usageHandler, usageOptions} from './usage.js';
import {deleteVarsHandler, deleteVarsOptions} from './vars/delete.js';
import {listVarsHandler, listVarsOptions} from './vars/list.js';
import {setVarsHandler, setVarsOptions} from './vars/set.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

async function main(argv: string[]): Promise<void> {
  const reflectCLI = createCLIParser(argv);

  try {
    await reflectCLI.parse();
  } catch (e) {
    if (e instanceof CommandLineArgsError) {
      console.log(e.message);
      await createCLIParser([...argv, '--help']).parse();
    } else {
      throw e;
    }
  }
}

function createCLIParser(argv: string[]) {
  const reflectCLI = createCLIParserBase(argv);

  reflectCLI.command(
    'create <name>',
    '🛠  Create a basic Reflect project',
    createOptions,
    handleWith(createHandler).andCleanup(),
  );

  reflectCLI.command(
    ['init', 'lfg'],
    '🚀 Add Reflect and basic mutators to an existing project',
    initOptions,
    handleWith(initHandler).andCleanup(),
  );

  reflectCLI.command(
    'dev',
    '💻 Start a local dev server for your Reflect project',
    devOptions,
    handleWith(devHandler).andCleanup(),
  );

  reflectCLI.command(
    'login',
    '🔓 Login to Reflect',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    handleWith(async (yargs: YargvToInterface<CommonYargsArgv>) => {
      await loginHandler(yargs);
      // authenticate() validates that credentials were written
      // and outputs the logged in user to the console.
      await authenticate(yargs);
    }).andCleanup(),
  );

  reflectCLI.command(
    'publish',
    '🌏 Publish your Reflect project',
    publishOptions,
    handleWith(publishHandler).andCleanup(),
  );

  reflectCLI.command(
    'status',
    '💡 Show the status of current deployed app',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    handleWith(statusHandler).andCleanup(),
  );

  reflectCLI.command(
    'tail',
    '🦚 Start a log tailing session',
    tailOptions,
    handleWith(tailHandler).andCleanup(),
  );

  reflectCLI.command('env', '🎛️  Manage environment variables', yargs => {
    yargs
      .option('dev', {
        describe: 'Manage local variables for `npx reflect dev`',
        type: 'boolean',
        default: false,
      })
      .command(
        'list',
        'List environment variables',
        listVarsOptions,
        handleWith(listVarsHandler).andCleanup(),
      )
      .command(
        'set <keysAndValues..>',
        'Set one or more environment variables',
        setVarsOptions,
        handleWith(setVarsHandler).andCleanup(),
      )
      .command(
        'delete <keys..>',
        'Delete one or more environment variables',
        deleteVarsOptions,
        handleWith(deleteVarsHandler).andCleanup(),
      )
      .demandCommand(1, 'Available commands:\n');
  });

  reflectCLI.command(
    'usage',
    '📊 Show usage summary (room time), with monthly, daily, or hourly breakdowns',
    usageOptions,
    handleWith(usageHandler).andCleanup(),
  );

  reflectCLI.command(
    'delete',
    '🗑️  Delete one or more Apps and their associated data. If no flags are specified, defaults to the App of the current directory.',
    deleteOptions,
    handleWith(deleteHandler).andCleanup(),
  );

  return reflectCLI;
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
main(hideBin(process.argv));
