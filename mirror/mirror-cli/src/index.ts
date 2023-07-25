import {
  CommandLineArgsError,
  createCLIParserBase,
} from 'reflect-cli/src/create-cli-parser.js';
import {hideBin} from 'yargs/helpers';
import {
  uploadReflectServerHandler,
  uploadReflectServerOptions,
} from './upload.js';

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

  // upload
  reflectCLI.command(
    'upload',
    '🆙 Build and upload @rocicorp/reflect/server to Firestore',
    uploadReflectServerOptions,
    uploadReflectServerHandler,
  );

  return reflectCLI;
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
main(hideBin(process.argv));
