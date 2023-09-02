import {hideBin} from 'yargs/helpers';
import {authenticate} from './auth-config.js';
import {
  CommandLineArgsError,
  createCLIParserBase,
} from './create-cli-parser.js';
import {createHandler, createOptions} from './create.js';
import {devHandler, devOptions} from './dev.js';
import {handleWith} from './firebase.js';
import {lfgHandler, lfgOptions} from './lfg.js';
import {loginHandler} from './login.js';
import {publishHandler, publishOptions} from './publish.js';
import {statusHandler} from './status.js';
import {tailHandler, tailOptions} from './tail/index.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {deleteHandler, deleteOptions} from './delete.js';

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

  // create
  reflectCLI.command(
    'create <name>',
    '🛠  Create, init and publish a basic Reflect project, ',
    createOptions,
    handleWith(createHandler).andCleanup(),
  );

  // init
  reflectCLI.command(
    ['init', 'lfg'],
    '🚀 Initialize and publish a basic Reflect project in the current directory',
    lfgOptions,
    handleWith(lfgHandler).andCleanup(),
  );

  // dev
  reflectCLI.command(
    'dev',
    '👷 Start a local dev server for your Reflect project',
    devOptions,
    handleWith(devHandler).andCleanup(),
  );

  // login
  reflectCLI.command(
    'login',
    '🔓 Login to Reflect',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    handleWith(async (yargs: YargvToInterface<CommonYargsArgv>) => {
      try {
        await loginHandler(yargs);
        // authenticate() validates that credentials were written
        // and outputs the logged in user to the console.
        await authenticate(yargs);
      } catch (e) {
        console.error(e);
      }
    }).andCleanup(),
  );

  // publish
  reflectCLI.command(
    'publish',
    '🆙 Publish your Reflect project',
    publishOptions,
    handleWith(publishHandler).andCleanup(),
  );

  // tail
  reflectCLI.command(
    'tail',
    '🦚 Starts a log tailing session',
    tailOptions,
    handleWith(tailHandler).andCleanup(),
  );

  // delete
  reflectCLI.command(
    'delete',
    '🗑️ Deletes one or more Apps and their associated data. If no flags are specified, defaults to the App of the current directory.',
    deleteOptions,
    handleWith(deleteHandler).andCleanup(),
  );

  reflectCLI.command(
    'status',
    false, // Don't show in help.
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    handleWith(statusHandler).andCleanup(),
  );

  return reflectCLI;
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
main(hideBin(process.argv));
