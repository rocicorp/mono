import {hideBin} from 'yargs/helpers';
import {addDeploymentsOptionsHandler} from './add-deployment-options.js';
import {
  backfillMetricsHandler,
  backfillMetricsOptions,
} from './backfill-metrics.js';
import {
  backupAnalyticsHandler,
  backupAnalyticsOptions,
} from './backup-analytics.js';
import {certificatesHandler, certificatesOptions} from './certificates.js';
import {
  checkProviderHandler,
  checkProviderOptions,
  configureProviderHandler,
  configureProviderOptions,
} from './configure-provider.js';
import {
  CommandLineArgsError,
  createCLIParserBase,
} from './create-cli-parser.js';
import {
  customHostnamesHandler,
  customHostnamesOptions,
} from './custom-hostnames.js';
import {
  deleteTeamSubdomainsHandler,
  deleteTeamSubdomainsOptions,
} from './delete-team-subdomains.js';
import {dnsRecordsHandler, dnsRecordsOptions} from './dns-records.js';
import {initFirebase} from './firebase.js';
import {
  genEncryptionKeyHandler,
  genEncryptionKeyOptions,
} from './gen-encryption-key.js';
import {getWorkerHandler, getWorkerOptions} from './get-worker.js';
import {grantSuperHandler, grantSuperOptions} from './grant-super.js';
import {
  listDeployedAppsHandler,
  listDeployedAppsOptions,
} from './list-deployed-apps.js';
import {
  migrateDnsCommentsToTagsHandler,
  migrateDnsCommentsToTagsOptions,
} from './migrate-dns-comments-to-tags.js';
import {
  migrateTeamLabelsHandler,
  migrateTeamLabelsOptions,
} from './migrate-team-labels.js';
import {migrateToEnvHandler, migrateToEnvOptions} from './migrate-to-env.js';
import {migrateToWFPHandler, migrateToWFPOptions} from './migrate-to-wfp.js';
import {
  migrateTotalMetricsHandler,
  migrateTotalMetricsOptions,
} from './migrate-total-metrics.js';
import {
  publishCustomDomainsHandler,
  publishCustomDomainsOptions,
} from './publish-custom-domains.js';
import {
  publishDispatcherHandler,
  publishDispatcherOptions,
} from './publish-dispatcher.js';
import {
  publishTailWorkersHandler,
  publishTailWorkersOptions,
} from './publish-tail-workers.js';
import {
  queryAnalyticsHandler,
  queryAnalyticsOptions,
} from './query-analytics.js';
import {
  releaseReflectServerHandler,
  releaseReflectServerOptions,
  revertReflectServerHandler,
  revertReflectServerOptions,
} from './release-server.js';
import {runQueryHandler} from './run-query.js';
import {sumUsageHandler, sumUsageOptions} from './sum-usage.js';
import {
  uploadReflectServerHandler,
  uploadReflectServerOptions,
} from './upload-server.js';
import {
  wipeDeploymentsHandler,
  wipeDeploymentsOptions,
} from './wipe-deployments.js';

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

  // unrelease-server
  reflectCLI.command(
    'unrelease-server',
    'Removes a server version from a set of server channels. The resulting highest server version will be re-deployed to apps in those channels.',
    revertReflectServerOptions,
    revertReflectServerHandler,
  );

  // grant-super
  reflectCLI.command(
    'grant-super <email>',
    'Grants temporary super powers (e.g. impersonation) to an account.',
    grantSuperOptions,
    grantSuperHandler,
  );

  // list-deployed-apps
  reflectCLI.command(
    'list-deployed-apps',
    'Lists hostnames of running apps.',
    listDeployedAppsOptions,
    listDeployedAppsHandler,
  );

  // configure-provider
  reflectCLI.command(
    'configure-provider [name]',
    'Configures a provider for hosting Workers.',
    configureProviderOptions,
    configureProviderHandler,
  );

  // check-provider
  reflectCLI.command(
    'check-provider',
    'Checks that the provider is properly set up.',
    checkProviderOptions,
    checkProviderHandler,
  );

  // gen-encryption-key
  reflectCLI.command(
    'gen-encryption-key',
    'Generates and stores the APP_SECRET_ENCRYPTION_KEY used for encrypting at-rest secrets.',
    genEncryptionKeyOptions,
    genEncryptionKeyHandler,
  );

  // wfp
  reflectCLI.command(
    'wfp <appID>',
    'Migrates an App to Workers for Platforms',
    migrateToWFPOptions,
    migrateToWFPHandler,
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

  // certs
  reflectCLI.command(
    'certs [pattern]',
    'Lists and optionally deletes certificate packs that match an optional pattern.',
    certificatesOptions,
    certificatesHandler,
  );

  // publish-dispatcher
  reflectCLI.command(
    'publish-dispatcher',
    'Publishes the mirror dispatcher for Workers for Platforms',
    publishDispatcherOptions,
    publishDispatcherHandler,
  );

  // publish-tail-workers
  reflectCLI.command(
    'publish-tail-workers',
    'Publishes the tail workers servicing app Workers',
    publishTailWorkersOptions,
    publishTailWorkersHandler,
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

  // backup-analytics
  reflectCLI.command(
    'backup-analytics <table>',
    'Execute a Worker Analytics SQL Query',
    backupAnalyticsOptions,
    backupAnalyticsHandler,
  );

  // backfill-metrics
  reflectCLI.command(
    'backfill-metrics',
    'Backfills aggregated metrics into Firestore. Also suitable for rerunning aggregations if Cloudflare Analytics data is delayed.',
    backfillMetricsOptions,
    backfillMetricsHandler,
  );

  reflectCLI.command(
    'migrate-total-metrics',
    'Migrates to the new total metrics format with monthly totals under each year',
    migrateTotalMetricsOptions,
    migrateTotalMetricsHandler,
  );

  // sum-usage
  reflectCLI.command(
    'sum-usage',
    'Sums the connection seconds over a period of time.',
    sumUsageOptions,
    sumUsageHandler,
  );

  reflectCLI.command(
    'migrate-to-env',
    'Splits Env data out of the App doc.',
    migrateToEnvOptions,
    migrateToEnvHandler,
  );

  reflectCLI.command(
    'migrate-team-labels',
    'Converts team subdomains to team labels. Triggers new deployments of those apps.',
    migrateTeamLabelsOptions,
    migrateTeamLabelsHandler,
  );

  reflectCLI.command(
    'migrate-dns-comments-to-tags',
    'Converts workaround-comments in DNSRecords to actual tags.',
    migrateDnsCommentsToTagsOptions,
    migrateDnsCommentsToTagsHandler,
  );

  reflectCLI.command(
    'delete-team-subdomains',
    'Deletes the team subdomains. They must have already been migrated by migrate-team-labels.',
    deleteTeamSubdomainsOptions,
    deleteTeamSubdomainsHandler,
  );

  reflectCLI.command(
    'run-query',
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
