import makeCLI, {Argv} from 'yargs';

export class CommandLineArgsError extends Error {}

export const scriptName = `npm run mirror`;

export function createCLIParserBase(argv: string[]): Argv<{
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
    .option('stack', {
      alias: 's',
      describe: 'The Firebase stack to execute on',
      choices: ['prod', 'staging'],
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

  reflectCLI.exitProcess(false);

  return reflectCLI;
}
