import {SemVer} from 'semver';
import makeCLI, {Argv} from 'yargs';
import {initFirebase} from './firebase.js';
import {tryDeprecationCheck, version} from './version.js';

export class CommandLineArgsError extends Error {}

export const scriptName = `npx reflect`;

export function createCLIParserBase(argv: string[]): Argv<{
  v: boolean | undefined;
  ['auth-key-from-env']: string | undefined;
  stack: string;
  local: boolean;
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
    .version(false) // This set to false to allow overwrite of default behavior
    .option('v', {
      describe: 'Show version number',
      alias: 'version',
      type: 'boolean',
    })
    .option('auth-key-from-env', {
      describe:
        'Authenticate with a value created with `npx reflect keys`, set in the specified environment variable',
      type: 'string',
      requiresArg: true,
    })
    .option('stack', {
      alias: 's',
      describe: 'prod or sandbox firebase stack',
      choices: ['prod', 'sandbox'],
      default: 'prod',
      requiresArg: true,
      hidden: true,
    })
    .option('local', {
      describe: 'run against a local auth login UI and cloud functions',
      type: 'boolean',
      default: false,
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
    } else if (args.v) {
      console.log(version);
    } else {
      reflectCLI.showHelp();
    }
  });

  // version
  reflectCLI.command('version', false, {}, () => {
    console.log(version);
  });

  reflectCLI.middleware(() => {
    const nodeVersion = new SemVer(process.versions.node);
    if (nodeVersion.major < 18) {
      console.log(
        `\nNode.js v18 or higher is required. (Current: v${nodeVersion})`,
      );
      console.log('Please update to newer version.\n');
      process.exit(-1);
    }
  });

  reflectCLI.middleware(async argv => {
    await tryDeprecationCheck(argv);
  });

  reflectCLI.middleware(argv => initFirebase(argv));

  reflectCLI.exitProcess(false);

  return reflectCLI;
}
