import {
  createAppKey,
  deleteAppKeys,
  editAppKey,
  listAppKeys,
} from 'mirror-protocol/src/app-keys.js';
import {deleteApp} from 'mirror-protocol/src/app.js';
import {publish} from 'mirror-protocol/src/publish.js';
import {deleteVars, listVars, setVars} from 'mirror-protocol/src/vars.js';
import {hideBin} from 'yargs/helpers';
import {authenticate} from './auth-config.js';
import {
  CommandLineArgsError,
  createCLIParserBase,
} from './create-cli-parser.js';
import {createHandler, createOptions} from './create.js';
import {deleteHandler, deleteOptions} from './delete.js';
import {devHandler, devOptions} from './dev.js';
import {authenticateAndHandleWith, handleWith} from './handler.js';
import {createAppKeyHandler, createAppKeyOptions} from './keys/create.js';
import {deleteAppKeysHandler, deleteAppKeysOptions} from './keys/delete.js';
import {editAppKeyHandler, editAppKeyOptions} from './keys/edit.js';
import {listAppKeysHandler, listAppKeysOptions} from './keys/list.js';
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
    authenticateAndHandleWith(createHandler).andCleanup(),
  );

  reflectCLI.command(
    'dev',
    '💻 Start a local dev server for your Reflect project',
    devOptions,
    authenticateAndHandleWith(devHandler).andCleanup(),
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
    authenticateAndHandleWith(publishHandler).withWarmup(publish).andCleanup(),
  );

  reflectCLI.command(
    'status',
    '💡 Show the status of current deployed app',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    authenticateAndHandleWith(statusHandler).andCleanup(),
  );

  reflectCLI.command(
    'tail',
    '🦚 Start a log tailing session',
    tailOptions,
    authenticateAndHandleWith(tailHandler).andCleanup(),
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
        authenticateAndHandleWith(listVarsHandler)
          .withWarmup(listVars)
          .andCleanup(),
      )
      .command(
        'set <keysAndValues..>',
        'Set one or more environment variables',
        setVarsOptions,
        authenticateAndHandleWith(setVarsHandler)
          .withWarmup(setVars)
          .andCleanup(),
      )
      .command(
        'delete <keys..>',
        'Delete one or more environment variables',
        deleteVarsOptions,
        authenticateAndHandleWith(deleteVarsHandler)
          .withWarmup(deleteVars)
          .andCleanup(),
      )
      .demandCommand(1, 'Available commands:\n');
  });

  reflectCLI.command('keys', '🔑 Manage app keys', yargs => {
    yargs
      .command(
        'list',
        'List app keys',
        listAppKeysOptions,
        authenticateAndHandleWith(listAppKeysHandler)
          .withWarmup(listAppKeys)
          .andCleanup(),
      )
      .command(
        'create <name>',
        'Create an app key',
        createAppKeyOptions,
        authenticateAndHandleWith(createAppKeyHandler)
          .withWarmup(listAppKeys, createAppKey)
          .andCleanup(),
      )
      .command(
        'edit <name>',
        'Edit an app key',
        editAppKeyOptions,
        authenticateAndHandleWith(editAppKeyHandler)
          .withWarmup(listAppKeys, editAppKey)
          .andCleanup(),
      )
      .command(
        'delete <names..>',
        'Delete one or more app keys',
        deleteAppKeysOptions,
        authenticateAndHandleWith(deleteAppKeysHandler)
          .withWarmup(deleteAppKeys)
          .andCleanup(),
      )
      .demandCommand(1, 'Available commands:\n');
  });

  reflectCLI.command(
    'usage',
    '📊 Show usage summary (room time), with monthly, daily, or hourly breakdowns',
    usageOptions,
    authenticateAndHandleWith(usageHandler).andCleanup(),
  );

  reflectCLI.command(
    'delete [name]',
    '🗑️  Delete one or more Apps. Defaults to the App of the current directory.',
    deleteOptions,
    authenticateAndHandleWith(deleteHandler).withWarmup(deleteApp).andCleanup(),
  );

  return reflectCLI;
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
main(hideBin(process.argv));
