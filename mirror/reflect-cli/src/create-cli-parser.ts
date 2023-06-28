import makeCLI, {Argv} from 'yargs';
import {version} from './version.js';

export class CommandLineArgsError extends Error {}

export function createCLIParserBase(argv: string[]): Argv<{
  v: boolean | undefined;
  config: string | undefined;
  env: string | undefined;
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

  reflectCLI.exitProcess(false);

  return reflectCLI;
}
