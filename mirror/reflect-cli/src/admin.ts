import {hideBin} from 'yargs/helpers';
import {
  uploadReflectServerHandler,
  uploadReflectServerOptions,
} from './admin/upload-reflect-server.js';
import {
  CommandLineArgsError,
  createCLIParserBase,
} from './create-cli-parser.js';

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

  // upload-reflect-server
  reflectCLI.command(
    'upload-reflect-server',
    '🆙 Build and upload @rocicorp/reflect/server to Firestore',
    uploadReflectServerOptions,
    uploadReflectServerHandler,
  );

  return reflectCLI;
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
main(hideBin(process.argv));
