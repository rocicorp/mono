import makeCLI, {Argv} from 'yargs';
import {version} from './version.js';
import {initFirebase} from './firebase.js';

export class CommandLineArgsError extends Error {}

export const scriptName = `npx @rocicorp/reflect`;

export function createCLIParserBase(argv: string[]): Argv<{
  v: boolean | undefined;
  config: string | undefined;
  env: string | undefined;
  stack: string;
}> {
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
    .scriptName(scriptName)
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
    })
    .option('stack', {
      alias: 's',
      describe: 'prod, staging, or local (emulator) stack to connect to',
      choices: ['prod', 'staging', 'local'],
      default: 'prod',
      requiresArg: true,
    });

  reflectCLI.help().alias('h', 'help');

  reflectCLI.command(['*'], false, {}, args => {
    if (args._.length > 0) {
      throw new CommandLineArgsError(`Unknown command: ${args._}.`);
    } else {
      if (args.v) {
        reflectCLI.showVersion();
      }
      reflectCLI.showHelp();
    }
  });

  // This set to false to allow overwrite of default behavior
  reflectCLI.version(false);

  // version
  reflectCLI.command('version', false, {}, () => {
    console.log(version);
  });

  reflectCLI.middleware(argv => {
    initFirebase(argv.stack);
  });

  reflectCLI.exitProcess(false);

  return reflectCLI;
}
