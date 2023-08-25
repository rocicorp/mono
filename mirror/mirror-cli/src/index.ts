import {
  CommandLineArgsError,
  createCLIParserBase,
} from 'reflect-cli/src/create-cli-parser.js';
import {hideBin} from 'yargs/helpers';
import {
  uploadReflectServerHandler,
  uploadReflectServerOptions,
} from './upload-server.js';
import {initializeApp} from 'firebase-admin/app';
import {
  wipeDeploymentsHandler,
  wipeDeploymentsOptions,
} from './wipe-deployments.js';
import {addDeploymentsOptionsHandler} from './add-deployment-options.js';
import {runQueryHandler} from './run-query.js';
import {
  releaseReflectServerHandler,
  releaseReflectServerOptions,
  revertReflectServerHandler,
  revertReflectServerOptions,
} from './release-server.js';

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

  reflectCLI.middleware(argv => {
    initializeApp({
      projectId:
        argv.stack === 'prod'
          ? 'reflect-mirror-prod'
          : 'reflect-mirror-staging',
    });
  });

  // uploadServer
  reflectCLI.command(
    'uploadServer',
    '🆙 Build and upload @rocicorp/reflect/server to Firestore',
    uploadReflectServerOptions,
    uploadReflectServerHandler,
  );

  // releaseServer
  reflectCLI.command(
    'releaseServer',
    'Deploy a server version to a set of server channels',
    releaseReflectServerOptions,
    releaseReflectServerHandler,
  );

  // unreleaseServer
  reflectCLI.command(
    'unreleaseServer',
    'Removes a server version to a set of server channels. The resulting highest server version will be re-deployed to apps in those channels.',
    revertReflectServerOptions,
    revertReflectServerHandler,
  );

  reflectCLI.command(
    'runQuery',
    'Runs a specific query against Firestore to see if an index is necessary (which would appear in an Error message)',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    runQueryHandler,
  );

  reflectCLI.command(
    'addDeploymentOptions',
    'Adds default deploymentsOptions to Apps that do not have them.',
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    () => {},
    addDeploymentsOptionsHandler,
  );

  reflectCLI.command(
    'wipeDeployments',
    'Wipes all deployments. Used only in staging while the schema is in flux.',
    wipeDeploymentsOptions,
    wipeDeploymentsHandler,
  );

  return reflectCLI;
}

// eslint-disable-next-line @typescript-eslint/no-floating-promises
main(hideBin(process.argv));
