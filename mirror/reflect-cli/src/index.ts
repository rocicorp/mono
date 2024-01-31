import {
  createApiKey,
  deleteApiKeys,
  editApiKey,
  listApiKeys,
} from 'mirror-protocol/src/api-keys.js';
import {deleteApp} from 'mirror-protocol/src/app.js';
import {publish} from 'mirror-protocol/src/publish.js';
import {deleteVars, listVars, setVars} from 'mirror-protocol/src/vars.js';
import {hideBin} from 'yargs/helpers';
import {appListHandler} from './apps.js';
import {authenticate} from './auth-config.js';
import {
  CommandLineArgsError,
  createCLIParserBase,
} from './create-cli-parser.js';
import {createHandler, createOptions} from './create.js';
import {deleteHandler, deleteOptions} from './delete.js';
import {devHandler, devOptions} from './dev.js';
import {AuthContext, authenticateAndHandleWith, handleWith} from './handler.js';
import {createKeyHandler, createKeyOptions} from './keys/create.js';
import {deleteKeysHandler, deleteKeysOptions} from './keys/delete.js';
import {editKeyHandler, editKeyOptions} from './keys/edit.js';
import {listKeysHandler, listKeysOptions} from './keys/list.js';
import {loginHandler} from './login.js';
import {publishHandler, publishOptions} from './publish.js';
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

  reflectCLI.command('apps', '📱 Manage Reflect apps', yargs => {
    yargs
      .command(
        'list',
        'List apps',
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        () => {},
        authenticateAndHandleWith(appListHandler).andCleanup(),
      )
      .command(
        'delete',
        '🗑️  Delete one or more Apps.',
        deleteOptions,
        authenticateAndHandleWith(deleteHandler)
          .withWarmup(deleteApp)
          .andCleanup(),
      )
      .demandCommand(1, 'Available commands:\n');
  });

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
    'tail',
    '🦚 Start a log tailing session',
    tailOptions,
    authenticateAndHandleWith(tailHandler).andCleanup(),
  );

  reflectCLI.command(
    'whoami',
    '💡 Show your team, provider, email and name',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    authenticateAndHandleWith(
      (
        _yargs: YargvToInterface<CommonYargsArgv>,
        authContext: AuthContext,
      ): Promise<void> => {
        console.log(
          `Team: ${authContext.user.additionalUserInfo?.username}\nProvider: ${authContext.user.additionalUserInfo?.providerId}\nEmail: ${authContext.user.email}\nName: ${authContext.user.additionalUserInfo?.profile?.name}`,
        );
        return Promise.resolve();
      },
    ).andCleanup(),
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

  reflectCLI.command(
    'keys',
    '🔑 Create and manage keys for automated tasks',
    yargs => {
      yargs
        .command(
          'list',
          'List keys',
          listKeysOptions,
          authenticateAndHandleWith(listKeysHandler)
            .withWarmup(listApiKeys)
            .andCleanup(),
        )
        .command(
          'create <name>',
          'Create a key',
          createKeyOptions,
          authenticateAndHandleWith(createKeyHandler)
            .withWarmup(listApiKeys, createApiKey)
            .andCleanup(),
        )
        .command(
          'edit <name>',
          'Edit a key',
          editKeyOptions,
          authenticateAndHandleWith(editKeyHandler)
            .withWarmup(listApiKeys, editApiKey)
            .andCleanup(),
        )
        .command(
          'delete <names..>',
          'Delete one or more keys',
          deleteKeysOptions,
          authenticateAndHandleWith(deleteKeysHandler)
            .withWarmup(deleteApiKeys)
            .andCleanup(),
        )
        .demandCommand(1, 'Available commands:\n');
    },
  );

  reflectCLI.command(
    'usage',
    '📊 Show usage summary (room time), with monthly, daily, or hourly breakdowns',
    usageOptions,
    authenticateAndHandleWith(usageHandler).andCleanup(),
  );

  return reflectCLI;
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
main(hideBin(process.argv));
