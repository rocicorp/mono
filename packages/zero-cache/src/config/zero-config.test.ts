import {stripVTControlCharacters as stripAnsi} from 'node:util';
import {expect, test, vi} from 'vitest';
import {
  parseOptions,
  parseOptionsAdvanced,
} from '../../../shared/src/options.ts';
import {INVALID_APP_ID_MESSAGE} from '../types/shards.ts';
import {zeroOptions} from './zero-config.ts';

class ExitAfterUsage extends Error {}
const exit = () => {
  throw new ExitAfterUsage();
};

// Tip: Rerun tests with -u to update the snapshot.
test('zero-cache --help', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptions(zeroOptions, ['--help'], 'ZERO_', {}, logger, exit),
  ).toThrow(ExitAfterUsage);
  expect(logger.info).toHaveBeenCalled();
  expect(stripAnsi(logger.info.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --upstream-db string                                        required                                                                                          
       ZERO_UPSTREAM_DB env                                                                                                                                        
                                                                 The "upstream" authoritative postgres database.                                                   
                                                                 In the future we will support other types of upstream besides PG.                                 
                                                                                                                                                                   
     --upstream-max-conns number                                 default: 20                                                                                       
       ZERO_UPSTREAM_MAX_CONNS env                                                                                                                                 
                                                                 The maximum number of connections to open to the upstream database                                
                                                                 for committing mutations. This is divided evenly amongst sync workers.                            
                                                                 In addition to this number, zero-cache uses one connection for the                                
                                                                 replication stream.                                                                               
                                                                                                                                                                   
                                                                 Note that this number must allow for at least one connection per                                  
                                                                 sync worker, or zero-cache will fail to start. See num-sync-workers                               
                                                                                                                                                                   
     --push-url string                                           optional                                                                                          
       ZERO_PUSH_URL env                                                                                                                                           
                                                                 The URL of the API server to which zero-cache will push mutations.                                
                                                                                                                                                                   
     --push-api-key string                                       optional                                                                                          
       ZERO_PUSH_API_KEY env                                                                                                                                       
                                                                 An optional secret used to authorize zero-cache to call the API server handling writes.           
                                                                                                                                                                   
     --pull-url string                                           optional                                                                                          
       ZERO_PULL_URL env                                                                                                                                           
                                                                 The URL of the API server to which zero-cache will send named queries.                            
                                                                                                                                                                   
     --pull-api-key string                                       optional                                                                                          
       ZERO_PULL_API_KEY env                                                                                                                                       
                                                                 An optional secret used to authorize zero-cache to call the API server handling reads.            
                                                                                                                                                                   
     --cvr-db string                                             optional                                                                                          
       ZERO_CVR_DB env                                                                                                                                             
                                                                 The Postgres database used to store CVRs. CVRs (client view records) keep track                   
                                                                 of the data synced to clients in order to determine the diff to send on reconnect.                
                                                                 If unspecified, the upstream-db will be used.                                                     
                                                                                                                                                                   
     --cvr-max-conns number                                      default: 30                                                                                       
       ZERO_CVR_MAX_CONNS env                                                                                                                                      
                                                                 The maximum number of connections to open to the CVR database.                                    
                                                                 This is divided evenly amongst sync workers.                                                      
                                                                                                                                                                   
                                                                 Note that this number must allow for at least one connection per                                  
                                                                 sync worker, or zero-cache will fail to start. See num-sync-workers                               
                                                                                                                                                                   
     --query-hydration-stats boolean                             optional                                                                                          
       ZERO_QUERY_HYDRATION_STATS env                                                                                                                              
                                                                 Track and log the number of rows considered by each query in the system.                          
                                                                 This is useful for debugging and performance tuning.                                              
                                                                                                                                                                   
     --change-db string                                          optional                                                                                          
       ZERO_CHANGE_DB env                                                                                                                                          
                                                                 The Postgres database used to store recent replication log entries, in order                      
                                                                 to sync multiple view-syncers without requiring multiple replication slots on                     
                                                                 the upstream database. If unspecified, the upstream-db will be used.                              
                                                                                                                                                                   
     --change-max-conns number                                   default: 5                                                                                        
       ZERO_CHANGE_MAX_CONNS env                                                                                                                                   
                                                                 The maximum number of connections to open to the change database.                                 
                                                                 This is used by the change-streamer for catching up                                               
                                                                 zero-cache replication subscriptions.                                                             
                                                                                                                                                                   
     --replica-file string                                       required                                                                                          
       ZERO_REPLICA_FILE env                                                                                                                                       
                                                                 File path to the SQLite replica that zero-cache maintains.                                        
                                                                 This can be lost, but if it is, zero-cache will have to re-replicate next                         
                                                                 time it starts up.                                                                                
                                                                                                                                                                   
     --replica-vacuum-interval-hours number                      optional                                                                                          
       ZERO_REPLICA_VACUUM_INTERVAL_HOURS env                                                                                                                      
                                                                 Performs a VACUUM at server startup if the specified number of hours has elapsed                  
                                                                 since the last VACUUM (or initial-sync). The VACUUM operation is heavyweight                      
                                                                 and requires double the size of the db in disk space. If unspecified, VACUUM                      
                                                                 operations are not performed.                                                                     
                                                                                                                                                                   
     --log-level debug,info,warn,error                           default: "info"                                                                                   
       ZERO_LOG_LEVEL env                                                                                                                                          
                                                                                                                                                                   
     --log-format text,json                                      default: "text"                                                                                   
       ZERO_LOG_FORMAT env                                                                                                                                         
                                                                 Use text for developer-friendly console logging                                                   
                                                                 and json for consumption by structured-logging services                                           
                                                                                                                                                                   
     --log-slow-row-threshold number                             default: 2                                                                                        
       ZERO_LOG_SLOW_ROW_THRESHOLD env                                                                                                                             
                                                                 The number of ms a row must take to fetch from table-source before it is considered slow.         
                                                                                                                                                                   
     --log-slow-hydrate-threshold number                         default: 100                                                                                      
       ZERO_LOG_SLOW_HYDRATE_THRESHOLD env                                                                                                                         
                                                                 The number of milliseconds a query hydration must take to print a slow warning.                   
                                                                                                                                                                   
     --log-ivm-sampling number                                   default: 5000                                                                                     
       ZERO_LOG_IVM_SAMPLING env                                                                                                                                   
                                                                 How often to collect IVM metrics. 1 out of N requests will be sampled where N is this value.      
                                                                                                                                                                   
     --app-id string                                             default: "zero"                                                                                   
       ZERO_APP_ID env                                                                                                                                             
                                                                 Unique identifier for the app.                                                                    
                                                                                                                                                                   
                                                                 Multiple zero-cache apps can run on a single upstream database, each of which                     
                                                                 is isolated from the others, with its own permissions, sharding (future feature),                 
                                                                 and change/cvr databases.                                                                         
                                                                                                                                                                   
                                                                 The metadata of an app is stored in an upstream schema with the same name,                        
                                                                 e.g. "zero", and the metadata for each app shard, e.g. client and mutation                        
                                                                 ids, is stored in the "{app-id}_{#}" schema. (Currently there is only a single                    
                                                                 "0" shard, but this will change with sharding).                                                   
                                                                                                                                                                   
                                                                 The CVR and Change data are managed in schemas named "{app-id}_{shard-num}/cvr"                   
                                                                 and "{app-id}_{shard-num}/cdc", respectively, allowing multiple apps and shards                   
                                                                 to share the same database instance (e.g. a Postgres "cluster") for CVR and Change management.    
                                                                                                                                                                   
                                                                 Due to constraints on replication slot names, an App ID may only consist of                       
                                                                 lower-case letters, numbers, and the underscore character.                                        
                                                                                                                                                                   
                                                                 Note that this option is used by both zero-cache and zero-deploy-permissions.                     
                                                                                                                                                                   
     --app-publications string[]                                 default: []                                                                                       
       ZERO_APP_PUBLICATIONS env                                                                                                                                   
                                                                 Postgres PUBLICATIONs that define the tables and columns to                                       
                                                                 replicate. Publication names may not begin with an underscore,                                    
                                                                 as zero reserves that prefix for internal use.                                                    
                                                                                                                                                                   
                                                                 If unspecified, zero-cache will create and use an internal publication that                       
                                                                 publishes all tables in the public schema, i.e.:                                                  
                                                                                                                                                                   
                                                                 CREATE PUBLICATION _{app-id}_public_0 FOR TABLES IN SCHEMA public;                                
                                                                                                                                                                   
                                                                 Note that changing the set of publications will result in resyncing the replica,                  
                                                                 which may involve downtime (replication lag) while the new replica is initializing.               
                                                                 To change the set of publications without disrupting an existing app, a new app                   
                                                                 should be created.                                                                                
                                                                                                                                                                   
     --auth-jwk string                                           optional                                                                                          
       ZERO_AUTH_JWK env                                                                                                                                           
                                                                 A public key in JWK format used to verify JWTs. Only one of jwk, jwksUrl and secret may be set.   
                                                                                                                                                                   
     --auth-jwks-url string                                      optional                                                                                          
       ZERO_AUTH_JWKS_URL env                                                                                                                                      
                                                                 A URL that returns a JWK set used to verify JWTs. Only one of jwk, jwksUrl and secret may be set. 
                                                                                                                                                                   
     --auth-secret string                                        optional                                                                                          
       ZERO_AUTH_SECRET env                                                                                                                                        
                                                                 A symmetric key used to verify JWTs. Only one of jwk, jwksUrl and secret may be set.              
                                                                                                                                                                   
     --port number                                               default: 4848                                                                                     
       ZERO_PORT env                                                                                                                                               
                                                                 The port for sync connections.                                                                    
                                                                                                                                                                   
     --change-streamer-mode dedicated,discover                   default: "dedicated"                                                                              
       ZERO_CHANGE_STREAMER_MODE env                                                                                                                               
                                                                 The mode for running or connecting to the change-streamer:                                        
                                                                 * dedicated: runs the change-streamer and shuts down when another                                 
                                                                       change-streamer takes over the replication slot. This is appropriate in a                   
                                                                       single-node configuration, or for the replication-manager in a                              
                                                                       multi-node configuration.                                                                   
                                                                 * discover: connects to the change-streamer as internally advertised in the                       
                                                                       change-db. This is appropriate for the view-syncers in a multi-node                         
                                                                       configuration.                                                                              
                                                                                                                                                                   
     --change-streamer-port number                               optional                                                                                          
       ZERO_CHANGE_STREAMER_PORT env                                                                                                                               
                                                                 The port on which the change-streamer runs. This is an internal                                   
                                                                 protocol between the replication-manager and view-syncers, which                                  
                                                                 runs in the same process tree in local development or a single-node configuration.                
                                                                                                                                                                   
                                                                 If unspecified, defaults to --port + 1.                                                           
                                                                                                                                                                   
     --change-streamer-address string                            optional                                                                                          
       ZERO_CHANGE_STREAMER_ADDRESS env                                                                                                                            
                                                                 The host:port for other processes to use when connecting to this                                  
                                                                 change-streamer. When unspecified, the machine's IP address and the                               
                                                                 --change-streamer-port will be advertised for discovery.                                          
                                                                                                                                                                   
                                                                 In most cases, the default behavior (unspecified) is sufficient, including in a                   
                                                                 single-node configuration or a multi-node configuration with host/awsvpc networking               
                                                                 (e.g. Fargate).                                                                                   
                                                                                                                                                                   
                                                                 For a multi-node configuration in which the process is unable to determine the                    
                                                                 externally addressable port (e.g. a container running with bridge mode networking),               
                                                                 the --change-streamer-address must be specified manually (e.g. a load balancer or                 
                                                                 service discovery address).                                                                       
                                                                                                                                                                   
     --task-id string                                            optional                                                                                          
       ZERO_TASK_ID env                                                                                                                                            
                                                                 Globally unique identifier for the zero-cache instance.                                           
                                                                                                                                                                   
                                                                 Setting this to a platform specific task identifier can be useful for debugging.                  
                                                                 If unspecified, zero-cache will attempt to extract the TaskARN if run from within                 
                                                                 an AWS ECS container, and otherwise use a random string.                                          
                                                                                                                                                                   
     --per-user-mutation-limit-max number                        optional                                                                                          
       ZERO_PER_USER_MUTATION_LIMIT_MAX env                                                                                                                        
                                                                 The maximum mutations per user within the specified windowMs.                                     
                                                                 If unset, no rate limiting is enforced.                                                           
                                                                                                                                                                   
     --per-user-mutation-limit-window-ms number                  default: 60000                                                                                    
       ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS env                                                                                                                  
                                                                 The sliding window over which the perUserMutationLimitMax is enforced.                            
                                                                                                                                                                   
     --num-sync-workers number                                   optional                                                                                          
       ZERO_NUM_SYNC_WORKERS env                                                                                                                                   
                                                                 The number of processes to use for view syncing.                                                  
                                                                 Leave this unset to use the maximum available parallelism.                                        
                                                                 If set to 0, the server runs without sync workers, which is the                                   
                                                                 configuration for running the replication-manager.                                                
                                                                                                                                                                   
     --auto-reset boolean                                        default: true                                                                                     
       ZERO_AUTO_RESET env                                                                                                                                         
                                                                 Automatically wipe and resync the replica when replication is halted.                             
                                                                 This situation can occur for configurations in which the upstream database                        
                                                                 provider prohibits event trigger creation, preventing the zero-cache from                         
                                                                 being able to correctly replicate schema changes. For such configurations,                        
                                                                 an upstream schema change will instead result in halting replication with an                      
                                                                 error indicating that the replica needs to be reset.                                              
                                                                                                                                                                   
                                                                 When auto-reset is enabled, zero-cache will respond to such situations                            
                                                                 by shutting down, and when restarted, resetting the replica and all synced                        
                                                                 clients. This is a heavy-weight operation and can result in user-visible                          
                                                                 slowness or downtime if compute resources are scarce.                                             
                                                                                                                                                                   
     --admin-password string                                     optional                                                                                          
       ZERO_ADMIN_PASSWORD env                                                                                                                                     
                                                                 A password used to administer zero-cache server, for example to access the                        
                                                                 /statz endpoint.                                                                                  
                                                                                                                                                                   
     --litestream-executable string                              optional                                                                                          
       ZERO_LITESTREAM_EXECUTABLE env                                                                                                                              
                                                                 Path to the litestream executable.                                                                
                                                                                                                                                                   
     --litestream-config-path string                             default: "./src/services/litestream/config.yml"                                                   
       ZERO_LITESTREAM_CONFIG_PATH env                                                                                                                             
                                                                 Path to the litestream yaml config file. zero-cache will run this with its                        
                                                                 environment variables, which can be referenced in the file via \${ENV}                             
                                                                 substitution, for example:                                                                        
                                                                 * ZERO_REPLICA_FILE for the db path                                                               
                                                                 * ZERO_LITESTREAM_BACKUP_LOCATION for the db replica url                                          
                                                                 * ZERO_LITESTREAM_LOG_LEVEL for the log level                                                     
                                                                 * ZERO_LOG_FORMAT for the log type                                                                
                                                                                                                                                                   
     --litestream-log-level debug,info,warn,error                default: "warn"                                                                                   
       ZERO_LITESTREAM_LOG_LEVEL env                                                                                                                               
                                                                                                                                                                   
     --litestream-backup-url string                              optional                                                                                          
       ZERO_LITESTREAM_BACKUP_URL env                                                                                                                              
                                                                 The location of the litestream backup, usually an s3:// URL.                                      
                                                                 This is only consulted by the replication-manager.                                                
                                                                 view-syncers receive this information from the replication-manager.                               
                                                                                                                                                                   
     --litestream-port number                                    optional                                                                                          
       ZERO_LITESTREAM_PORT env                                                                                                                                    
                                                                 Port on which litestream exports metrics, used to determine the replication                       
                                                                 watermark up to which it is safe to purge change log records.                                     
                                                                                                                                                                   
                                                                 If unspecified, defaults to --port + 2.                                                           
                                                                                                                                                                   
     --litestream-checkpoint-threshold-mb number                 default: 40                                                                                       
       ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB env                                                                                                                 
                                                                 The size of the WAL file at which to perform an SQlite checkpoint to apply                        
                                                                 the writes in the WAL to the main database file. Each checkpoint creates                          
                                                                 a new WAL segment file that will be backed up by litestream. Smaller thresholds                   
                                                                 may improve read performance, at the expense of creating more files to download                   
                                                                 when restoring the replica from the backup.                                                       
                                                                                                                                                                   
     --litestream-incremental-backup-interval-minutes number     default: 15                                                                                       
       ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES env                                                                                                     
                                                                 The interval between incremental backups of the replica. Shorter intervals                        
                                                                 reduce the amount of change history that needs to be replayed when catching                       
                                                                 up a new view-syncer, at the expense of increasing the number of files needed                     
                                                                 to download for the initial litestream restore.                                                   
                                                                                                                                                                   
     --litestream-snapshot-backup-interval-hours number          default: 12                                                                                       
       ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_HOURS env                                                                                                          
                                                                 The interval between snapshot backups of the replica. Snapshot backups                            
                                                                 make a full copy of the database to a new litestream generation. This                             
                                                                 improves restore time at the expense of bandwidth. Applications with a                            
                                                                 large database and low write rate can increase this interval to reduce                            
                                                                 network usage for backups (litestream defaults to 24 hours).                                      
                                                                                                                                                                   
     --litestream-restore-parallelism number                     default: 48                                                                                       
       ZERO_LITESTREAM_RESTORE_PARALLELISM env                                                                                                                     
                                                                 The number of WAL files to download in parallel when performing the                               
                                                                 initial restore of the replica from the backup.                                                   
                                                                                                                                                                   
     --litestream-multipart-concurrency number                   default: 48                                                                                       
       ZERO_LITESTREAM_MULTIPART_CONCURRENCY env                                                                                                                   
                                                                 The number of parts (of size --litestream-multipart-size bytes)                                   
                                                                 to upload or download in parallel when backing up or restoring the snapshot.                      
                                                                                                                                                                   
     --litestream-multipart-size number                          default: 16777216                                                                                 
       ZERO_LITESTREAM_MULTIPART_SIZE env                                                                                                                          
                                                                 The size of each part when uploading or downloading the snapshot with                             
                                                                 --multipart-concurrency. Note that up to concurrency * size                                       
                                                                 bytes of memory are used when backing up or restoring the snapshot.                               
                                                                                                                                                                   
     --storage-db-tmp-dir string                                 optional                                                                                          
       ZERO_STORAGE_DB_TMP_DIR env                                                                                                                                 
                                                                 tmp directory for IVM operator storage. Leave unset to use os.tmpdir()                            
                                                                                                                                                                   
     --initial-sync-table-copy-workers number                    default: 5                                                                                        
       ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS env                                                                                                                    
                                                                 The number of parallel workers used to copy tables during initial sync.                           
                                                                 Each worker uses a database connection and will buffer up to (approximately)                      
                                                                 10 MB of table data in memory during initial sync. Increasing the number of                       
                                                                 workers may improve initial sync speed; however, note that local disk throughput                  
                                                                 (i.e. IOPS), upstream CPU, and network bandwidth may also be bottlenecks.                         
                                                                                                                                                                   
     --target-client-row-count number                            default: 20000                                                                                    
       ZERO_TARGET_CLIENT_ROW_COUNT env                                                                                                                            
                                                                 The target number of rows to keep per client in the client side cache.                            
                                                                 This limit is a soft limit. When the number of rows in the cache exceeds                          
                                                                 this limit, zero-cache will evict inactive queries in order of ttl-based expiration.              
                                                                 Active queries, on the other hand, are never evicted and are allowed to use more                  
                                                                 rows than the limit.                                                                              
                                                                                                                                                                   
     --lazy-startup boolean                                      default: false                                                                                    
       ZERO_LAZY_STARTUP env                                                                                                                                       
                                                                 Delay starting the majority of zero-cache until first request.                                    
                                                                                                                                                                   
                                                                 This is mainly intended to avoid connecting to Postgres replication stream                        
                                                                 until the first request is received, which can be useful i.e., for preview instances.             
                                                                                                                                                                   
                                                                 Currently only supported in single-node mode.                                                     
                                                                                                                                                                   
     --server-version string                                     optional                                                                                          
       ZERO_SERVER_VERSION env                                                                                                                                     
                                                                 The version string outputted to logs when the server starts up.                                   
                                                                                                                                                                   
    "
  `);
});

test.each([['has/slashes'], ['has-dashes'], ['has.dots']])(
  '--app-id %s',
  appID => {
    const logger = {info: vi.fn()};
    expect(() =>
      parseOptionsAdvanced(
        zeroOptions,
        ['--app-id', appID],
        'ZERO_',
        false, // allow unknown
        true, // allow partial
        {},
        logger,
        exit,
      ),
    ).toThrowError(INVALID_APP_ID_MESSAGE);
  },
);

test.each([['isok'], ['has_underscores'], ['1'], ['123']])(
  '--app-id %s',
  appID => {
    const {config} = parseOptionsAdvanced(
      zeroOptions,
      ['--app-id', appID],
      'ZERO_',
      false,
      true,
    );
    expect(config.app.id).toBe(appID);
  },
);

test('--shard-id disallowed', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptionsAdvanced(
      zeroOptions,
      ['--shard-id', 'prod'],
      'ZERO_',
      false, // allow unknown
      true, // allow partial
      {},
      logger,
      exit,
    ),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: ZERO_SHARD_ID is deprecated. Please use ZERO_APP_ID instead.]`,
  );
});
