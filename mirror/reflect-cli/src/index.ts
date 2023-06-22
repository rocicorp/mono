import process from 'process';
import makeCLI from 'yargs';
import {hideBin} from 'yargs/helpers';
import {publishHandler, publishOptions} from './publish.js';
import {version} from './version.js';
import {loginHandler} from './login.js';
import {statusHandler} from './status.js';

export class CommandLineArgsError extends Error {}

export function createCLIParser(argv: string[]) {
  // Type check result against CommonYargsOptions to make sure we've included
  // all common options
  const reflectCLI = makeCLI(argv)
    .strict()
    .showHelpOnFail(true)
    .fail((msg, error) => {
      if (!error || error.name === 'YError') {
        error = new CommandLineArgsError(msg);
      }
      throw error;
    })
    .scriptName(`npx @rocicorp/reflect`)
    .wrap(null)
    .version(false)
    .option('v', {
      describe: 'Show version number',
      alias: 'version',
      type: 'boolean',
    })
    .option('config', {
      alias: 'c',
      describe: 'Path to .toml configuration file',
      type: 'string',
      requiresArg: true,
    })
    .option('env', {
      alias: 'e',
      describe: 'Environment to use for operations and .env files',
      type: 'string',
      requiresArg: true,
    });

  reflectCLI.help().alias('h', 'help');

  reflectCLI.command(
    ['*'],
    false,
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    args => {
      if (args._.length > 0) {
        throw new CommandLineArgsError(`Unknown command: ${args._}.`);
      } else {
        if (args.v) {
          reflectCLI.showVersion();
        }
        reflectCLI.showHelp();
      }
    },
  );

  // init
  reflectCLI.command(
    'init [name]',
    '📥 Initialize a basic Reflect project, ',
    //initOptions,
    //initHandler
  );

  // login
  reflectCLI.command(
    'login',
    '🔓 Login to Reflect',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    async () => {
      try {
        await loginHandler();
      } catch (e) {
        console.error(e);
      }
    },
  );

  reflectCLI.command(
    'status',
    '🚥 Get your status',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    statusHandler,
  );

  // dev
  reflectCLI.command(
    'dev [script]',
    '👂 Start a local server for developing your ',
    // devOptions,
    // devHandler
  );

  // publish
  reflectCLI.command(
    'publish <script>',
    '🆙 Publish your reflect project',
    publishOptions,
    publishHandler,
  );

  // tail
  reflectCLI.command(
    'tail [worker]',
    '🦚 Starts a log tailing session ruinning worker',
    // tailOptions,
    // tailHandler
  );

  // This set to false to allow overwrite of default behaviour
  reflectCLI.version(false);

  // version
  reflectCLI.command(
    'version',
    false,
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    () => {
      console.log(version);
    },
  );

  reflectCLI.exitProcess(false);

  return reflectCLI;
}

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
// eslint-disable-next-line @typescript-eslint/no-floating-promises
main(hideBin(process.argv));
