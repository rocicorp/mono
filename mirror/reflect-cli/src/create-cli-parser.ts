import makeCLI, {Argv} from 'yargs';
import {initFirebase} from './firebase.js';
import {version} from './version.js';

export class CommandLineArgsError extends Error {}

export const scriptName = `npx @rocicorp/reflect`;

export function createCLIParserBase(argv: string[]): Argv<{
  v: boolean | undefined;
  stack: string;
  runAs: string | undefined;
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
    .option('stack', {
      alias: 's',
      describe: 'prod, staging, or local (emulator) stack to connect to',
      choices: ['prod', 'staging', 'local'],
      default: 'prod',
      requiresArg: true,
      hidden: true,
    })
    .option('runAs', {
      describe: 'User ID to run as, delegation permitting',
      type: 'string',
      requiresArg: true,
      hidden: true,
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
