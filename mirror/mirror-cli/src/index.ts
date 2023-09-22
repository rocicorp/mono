import {
  CommandLineArgsError,
  createCLIParserBase,
} from './create-cli-parser.js';
import {hideBin} from 'yargs/helpers';
import {
  uploadReflectServerHandler,
  uploadReflectServerOptions,
} from './upload-server.js';
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
import {
  migrateTeamLabelsHandler,
  migrateTeamLabelsOptions,
} from './migrate-team-labels.js';
import {grantSuperHandler, grantSuperOptions} from './grant-super.js';
import {initFirebase} from './firebase.js';
import {
  queryAnalyticsHandler,
  queryAnalyticsOptions,
} from './query-analytics.js';
import {
  publishCustomDomainsHandler,
  publishCustomDomainsOptions,
} from './publish-custom-domains.js';
import {dnsRecordsHandler, dnsRecordsOptions} from './dns-records.js';
import {
  customHostnamesHandler,
  customHostnamesOptions,
} from './custom-hostnames.js';
import {
  publishDispatcherHandler,
  publishDispatcherOptions,
} from './publish-dispatcher.js';
import {getWorkerHandler, getWorkerOptions} from './get-worker.js';
import {
  deleteTeamSubdomainsHandler,
  deleteTeamSubdomainsOptions,
} from './delete-team-subdomains.js';

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

  reflectCLI.middleware(argv => initFirebase(argv));

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
    'Removes a server version from a set of server channels. The resulting highest server version will be re-deployed to apps in those channels.',
    revertReflectServerOptions,
    revertReflectServerHandler,
  );

  // grantSuper
  reflectCLI.command(
    'grantSuper <email>',
    'Grants temporary super powers (e.g. impersonation) to an account.',
    grantSuperOptions,
    grantSuperHandler,
  );

  // publish-custom-domain
  reflectCLI.command(
    'publish-custom-domains <script-name> [domains..]',
    'Points the specified custom domains to a script.',
    publishCustomDomainsOptions,
    publishCustomDomainsHandler,
  );

  // custom-hostnames
  reflectCLI.command(
    'custom-hostnames [pattern]',
    'Lists and optionally deletes custom hostnames records that match an optional pattern.',
    customHostnamesOptions,
    customHostnamesHandler,
  );

  // dns-records
  reflectCLI.command(
    'dns-records [search]',
    'Lists and optionally deletes DNS records that match an optional pattern.',
    dnsRecordsOptions,
    dnsRecordsHandler,
  );

  // publish-dispatcher
  reflectCLI.command(
    'publish-dispatcher',
    'Publishes the mirror dispatcher for Workers for Platforms',
    publishDispatcherOptions,
    publishDispatcherHandler,
  );

  // get-worker
  reflectCLI.command(
    'get-worker <name> [component]',
    'Gets the script or a component thereof.',
    getWorkerOptions,
    getWorkerHandler,
  );

  // query-analytics
  reflectCLI.command(
    'query-analytics <query>',
    'Execute a Worker Analytics SQL Query',
    queryAnalyticsOptions,
    queryAnalyticsHandler,
  );

  reflectCLI.command(
    'migrate-team-labels',
    'Converts team subdomains to team labels. Triggers new deployments of those apps.',
    migrateTeamLabelsOptions,
    migrateTeamLabelsHandler,
  );

  reflectCLI.command(
    'delete-team-subdomains',
    'Deletes the team subdomains. They must have already been migrated by migrate-team-labels.',
    deleteTeamSubdomainsOptions,
    deleteTeamSubdomainsHandler,
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
