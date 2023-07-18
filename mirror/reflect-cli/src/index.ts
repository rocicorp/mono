import {hideBin} from 'yargs/helpers';
import {
  CommandLineArgsError,
  createCLIParserBase,
} from './create-cli-parser.js';
import {initHandler, initOptions} from './init.js';
import {loginHandler} from './login.js';
import {publishHandler, publishOptions} from './publish.js';
import {statusHandler} from './status.js';

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

  // init
  reflectCLI.command(
    'init [name]',
    '📥 Initialize a basic Reflect project, ',
    initOptions,
    initHandler,
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

  // tail
  reflectCLI.command(
    'tail [worker]',
    '🦚 Starts a log tailing session running worker',
    // tailOptions,
    // tailHandler
  );

  // publish
  reflectCLI.command(
    'publish <script>',
    '🆙 Publish your reflect project',
    publishOptions,
    publishHandler,
  );

  return reflectCLI;
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
main(hideBin(process.argv));
