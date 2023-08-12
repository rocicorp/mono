import {hideBin} from 'yargs/helpers';
import {authenticate} from './auth-config.js';
import {
  CommandLineArgsError,
  createCLIParserBase,
} from './create-cli-parser.js';
import {createHandler, createOptions} from './create.js';
import {devHandler, devOptions} from './dev.js';
import {handleWith} from './firebase.js';
import {initHandler, initOptions} from './init.js';
import {loginHandler} from './login.js';
import {publishHandler, publishOptions} from './publish.js';
import {statusHandler} from './status.js';
import {tailHandler, tailOptions} from './tail.js';

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
    'init [name]',
    '📥 Initialize a basic Reflect project, ',
    initOptions,
    handleWith(initHandler).andCleanup(),
  );

  reflectCLI.command(
    'create <name>',
    '🛠 Create, init and publish a basic Reflect project, ',
    createOptions,
    handleWith(createHandler).andCleanup(),
  );

  // login
  reflectCLI.command(
    'login',
    '🔓 Login to Reflect',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    handleWith(async () => {
      try {
        await loginHandler();
        // authenticate() validates that credentials were written
        // and outputs the logged in user to the console.
        await authenticate();
      } catch (e) {
        console.error(e);
      }
    }).andCleanup(),
  );

  reflectCLI.command(
    'status',
    '🚥 Get your status',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    handleWith(statusHandler).andCleanup(),
  );

  // dev
  reflectCLI.command(
    'dev <script>',
    '👷 Start a local dev server for your Reflect project',
    devOptions,
    handleWith(devHandler).andCleanup(),
  );

  // tail
  reflectCLI.command(
    'tail [worker]',
    '🦚 Starts a log tailing session running worker',
    tailOptions,
    handleWith(tailHandler).andCleanup(),
  );

  // publish
  reflectCLI.command(
    'publish <script>',
    '🆙 Publish your reflect project',
    publishOptions,
    handleWith(publishHandler).andCleanup(),
  );

  return reflectCLI;
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
main(hideBin(process.argv));
