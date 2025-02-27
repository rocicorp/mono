import {stripVTControlCharacters as stripAnsi} from 'node:util';
import {expect, test, vi} from 'vitest';
import {parseOptions} from '../../../shared/src/options.ts';
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
     --upstream-db string                                       required                                                                                          
       ZERO_UPSTREAM_DB env                                                                                                                                       
                                                                The "upstream" authoritative postgres database.                                                   
                                                                In the future we will support other types of upstream besides PG.                                 
                                                                                                                                                                  
     --upstream-max-conns number                                default: 20                                                                                       
       ZERO_UPSTREAM_MAX_CONNS env                                                                                                                                
                                                                The maximum number of connections to open to the upstream database                                
                                                                for committing mutations. This is divided evenly amongst sync workers.                            
                                                                In addition to this number, zero-cache uses one connection for the                                
                                                                replication stream.                                                                               
                                                                                                                                                                  
                                                                Note that this number must allow for at least one connection per                                  
                                                                sync worker, or zero-cache will fail to start. See num-sync-workers                               
                                                                                                                                                                  
     --push-url string                                          optional                                                                                          
       ZERO_PUSH_URL env                                                                                                                                          
                                                                The URL of the API server to which zero-cache will push mutations.                                
                                                                                                                                                                  
     --push-api-key string                                      optional                                                                                          
       ZERO_PUSH_API_KEY env                                                                                                                                      
                                                                An optional secret used to authorize zero-cache to call the API server.                           
                                                                                                                                                                  
     --cvr-db string                                            required                                                                                          
       ZERO_CVR_DB env                                                                                                                                            
                                                                A separate Postgres database we use to store CVRs. CVRs (client view records)                     
                                                                keep track of which clients have which data. This is how we know what diff to                     
                                                                send on reconnect. It can be same database as above, but it makes most sense                      
                                                                for it to be a separate "database" in the same postgres "cluster".                                
                                                                                                                                                                  
     --cvr-max-conns number                                     default: 30                                                                                       
       ZERO_CVR_MAX_CONNS env                                                                                                                                     
                                                                The maximum number of connections to open to the CVR database.                                    
                                                                This is divided evenly amongst sync workers.                                                      
                                                                                                                                                                  
                                                                Note that this number must allow for at least one connection per                                  
                                                                sync worker, or zero-cache will fail to start. See num-sync-workers                               
                                                                                                                                                                  
     --query-hydration-stats boolean                            optional                                                                                          
       ZERO_QUERY_HYDRATION_STATS env                                                                                                                             
                                                                Track and log the number of rows considered by each query in the system.                          
                                                                This is useful for debugging and performance tuning.                                              
                                                                                                                                                                  
     --change-db string                                         required                                                                                          
       ZERO_CHANGE_DB env                                                                                                                                         
                                                                Yet another Postgres database, used to store a replication log.                                   
                                                                                                                                                                  
     --change-max-conns number                                  default: 5                                                                                        
       ZERO_CHANGE_MAX_CONNS env                                                                                                                                  
                                                                The maximum number of connections to open to the change database.                                 
                                                                This is used by the change-streamer for catching up                                               
                                                                zero-cache replication subscriptions.                                                             
                                                                                                                                                                  
     --replica-file string                                      required                                                                                          
       ZERO_REPLICA_FILE env                                                                                                                                      
                                                                File path to the SQLite replica that zero-cache maintains.                                        
                                                                This can be lost, but if it is, zero-cache will have to re-replicate next                         
                                                                time it starts up.                                                                                
                                                                                                                                                                  
     --log-level debug,info,warn,error                          default: "info"                                                                                   
       ZERO_LOG_LEVEL env                                                                                                                                         
                                                                                                                                                                  
     --log-format text,json                                     default: "text"                                                                                   
       ZERO_LOG_FORMAT env                                                                                                                                        
                                                                Use text for developer-friendly console logging                                                   
                                                                and json for consumption by structured-logging services                                           
                                                                                                                                                                  
     --log-trace-collector string                               optional                                                                                          
       ZERO_LOG_TRACE_COLLECTOR env                                                                                                                               
                                                                The URL of the trace collector to which to send trace data. Traces are sent over http.            
                                                                Port defaults to 4318 for most collectors.                                                        
                                                                                                                                                                  
     --log-slow-row-threshold number                            default: 2                                                                                        
       ZERO_LOG_SLOW_ROW_THRESHOLD env                                                                                                                            
                                                                The number of ms a row must take to fetch from table-source before it is considered slow.         
                                                                                                                                                                  
     --log-ivm-sampling number                                  default: 5000                                                                                     
       ZERO_LOG_IVM_SAMPLING env                                                                                                                                  
                                                                How often to collect IVM metrics. 1 out of N requests will be sampled where N is this value.      
                                                                                                                                                                  
     --shard-id string                                          default: "0"                                                                                      
       ZERO_SHARD_ID env                                                                                                                                          
                                                                Unique identifier for the zero-cache shard.                                                       
                                                                                                                                                                  
                                                                A shard presents a logical partition of the upstream database, delineated                         
                                                                by a set of publications and managed by a dedicated replication slot.                             
                                                                                                                                                                  
                                                                A shard's zero clients table and shard-internal functions are stored in                           
                                                                the zero_{id} schema in the upstream database.                                                    
                                                                                                                                                                  
                                                                Due to constraints on replication slot names, a shard ID may only consist of                      
                                                                lower-case letters, numbers, and the underscore character.                                        
                                                                                                                                                                  
     --shard-publications string[]                              default: []                                                                                       
       ZERO_SHARD_PUBLICATIONS env                                                                                                                                
                                                                Postgres PUBLICATIONs that define the partition of the upstream                                   
                                                                replicated to the shard. Publication names may not begin with an underscore,                      
                                                                as zero reserves that prefix for internal use.                                                    
                                                                                                                                                                  
                                                                If unspecified, zero-cache will create and use an internal publication that                       
                                                                publishes all tables in the public schema, i.e.:                                                  
                                                                                                                                                                  
                                                                CREATE PUBLICATION _zero_public_0 FOR TABLES IN SCHEMA public;                                    
                                                                                                                                                                  
                                                                Note that once a shard has begun syncing data, this list of publications                          
                                                                cannot be changed, and zero-cache will refuse to start if a specified                             
                                                                value differs from what was originally synced.                                                    
                                                                                                                                                                  
                                                                To use a different set of publications, a new shard should be created.                            
                                                                                                                                                                  
     --auth-jwk string                                          optional                                                                                          
       ZERO_AUTH_JWK env                                                                                                                                          
                                                                A public key in JWK format used to verify JWTs. Only one of jwk, jwksUrl and secret may be set.   
                                                                                                                                                                  
     --auth-jwks-url string                                     optional                                                                                          
       ZERO_AUTH_JWKS_URL env                                                                                                                                     
                                                                A URL that returns a JWK set used to verify JWTs. Only one of jwk, jwksUrl and secret may be set. 
                                                                                                                                                                  
     --auth-secret string                                       optional                                                                                          
       ZERO_AUTH_SECRET env                                                                                                                                       
                                                                A symmetric key used to verify JWTs. Only one of jwk, jwksUrl and secret may be set.              
                                                                                                                                                                  
     --port number                                              default: 4848                                                                                     
       ZERO_PORT env                                                                                                                                              
                                                                The port for sync connections.                                                                    
                                                                                                                                                                  
     --change-streamer-port number                              optional                                                                                          
       ZERO_CHANGE_STREAMER_PORT env                                                                                                                              
                                                                The port on which the change-streamer runs. This is an internal                                   
                                                                protocol between the replication-manager and zero-cache, which                                    
                                                                runs in the same process in local development.                                                    
                                                                                                                                                                  
                                                                If unspecified, defaults to --port + 1.                                                           
                                                                                                                                                                  
     --task-id string                                           optional                                                                                          
       ZERO_TASK_ID env                                                                                                                                           
                                                                Globally unique identifier for the zero-cache instance.                                           
                                                                                                                                                                  
                                                                Setting this to a platform specific task identifier can be useful for debugging.                  
                                                                If unspecified, zero-cache will attempt to extract the TaskARN if run from within                 
                                                                an AWS ECS container, and otherwise use a random string.                                          
                                                                                                                                                                  
     --per-user-mutation-limit-max number                       optional                                                                                          
       ZERO_PER_USER_MUTATION_LIMIT_MAX env                                                                                                                       
                                                                The maximum mutations per user within the specified windowMs.                                     
                                                                If unset, no rate limiting is enforced.                                                           
                                                                                                                                                                  
     --per-user-mutation-limit-window-ms number                 default: 60000                                                                                    
       ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS env                                                                                                                 
                                                                The sliding window over which the perUserMutationLimitMax is enforced.                            
                                                                                                                                                                  
     --num-sync-workers number                                  optional                                                                                          
       ZERO_NUM_SYNC_WORKERS env                                                                                                                                  
                                                                The number of processes to use for view syncing.                                                  
                                                                Leave this unset to use the maximum available parallelism.                                        
                                                                If set to 0, the server runs without sync workers, which is the                                   
                                                                configuration for running the replication-manager.                                                
                                                                                                                                                                  
     --change-streamer-uri string                               optional                                                                                          
       ZERO_CHANGE_STREAMER_URI env                                                                                                                               
                                                                When unset, the zero-cache runs its own replication-manager                                       
                                                                (i.e. change-streamer). In production, this should be set to                                      
                                                                the replication-manager URI, which runs a change-streamer                                         
                                                                on port 4849.                                                                                     
                                                                                                                                                                  
     --auto-reset boolean                                       default: true                                                                                     
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
                                                                                                                                                                  
     --litestream-executable string                             optional                                                                                          
       ZERO_LITESTREAM_EXECUTABLE env                                                                                                                             
                                                                Path to the litestream executable. This option has no effect if                                   
                                                                litestream-backup-url is unspecified.                                                             
                                                                                                                                                                  
     --litestream-config-path string                            default: "./src/services/litestream/config.yml"                                                   
       ZERO_LITESTREAM_CONFIG_PATH env                                                                                                                            
                                                                Path to the litestream yaml config file. zero-cache will run this with its                        
                                                                environment variables, which can be referenced in the file via \${ENV}                             
                                                                substitution, for example:                                                                        
                                                                * ZERO_REPLICA_FILE for the db path                                                               
                                                                * ZERO_LITESTREAM_BACKUP_LOCATION for the db replica url                                          
                                                                * ZERO_LITESTREAM_LOG_LEVEL for the log level                                                     
                                                                * ZERO_LOG_FORMAT for the log type                                                                
                                                                                                                                                                  
     --litestream-log-level debug,info,warn,error               default: "warn"                                                                                   
       ZERO_LITESTREAM_LOG_LEVEL env                                                                                                                              
                                                                                                                                                                  
     --litestream-backup-url string                             optional                                                                                          
       ZERO_LITESTREAM_BACKUP_URL env                                                                                                                             
                                                                The location of the litestream backup, usually an s3:// URL.                                      
                                                                If set, the litestream-executable must also be specified.                                         
                                                                                                                                                                  
     --litestream-checkpoint-threshold-mb number                default: 40                                                                                       
       ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB env                                                                                                                
                                                                The size of the WAL file at which to perform an SQlite checkpoint to apply                        
                                                                the writes in the WAL to the main database file. Each checkpoint creates                          
                                                                a new WAL segment file that will be backed up by litestream. Smaller thresholds                   
                                                                may improve read performance, at the expense of creating more files to download                   
                                                                when restoring the replica from the backup.                                                       
                                                                                                                                                                  
     --litestream-incremental-backup-interval-minutes number    default: 15                                                                                       
       ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES env                                                                                                    
                                                                The interval between incremental backups of the replica. Shorter intervals                        
                                                                reduce the amount of change history that needs to be replayed when catching                       
                                                                up a new view-syncer, at the expense of increasing the number of files needed                     
                                                                to download for the initial litestream restore.                                                   
                                                                                                                                                                  
     --litestream-snapshot-backup-interval-hours number         default: 12                                                                                       
       ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_HOURS env                                                                                                         
                                                                The interval between snapshot backups of the replica. Snapshot backups                            
                                                                make a full copy of the database to a new litestream generation. This                             
                                                                improves restore time at the expense of bandwidth. Applications with a                            
                                                                large database and low write rate can increase this interval to reduce                            
                                                                network usage for backups (litestream defaults to 24 hours).                                      
                                                                                                                                                                  
     --litestream-restore-parallelism number                    default: 48                                                                                       
       ZERO_LITESTREAM_RESTORE_PARALLELISM env                                                                                                                    
                                                                The number of WAL files to download in parallel when performing the                               
                                                                initial restore of the replica from the backup.                                                   
                                                                                                                                                                  
     --storage-db-tmp-dir string                                optional                                                                                          
       ZERO_STORAGE_DB_TMP_DIR env                                                                                                                                
                                                                tmp directory for IVM operator storage. Leave unset to use os.tmpdir()                            
                                                                                                                                                                  
     --initial-sync-table-copy-workers number                   default: 5                                                                                        
       ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS env                                                                                                                   
                                                                The number of parallel workers used to copy tables during initial sync.                           
                                                                Each worker copies a single table at a time, fetching rows in batches of                          
                                                                of initial-sync-row-batch-size.                                                                   
                                                                                                                                                                  
     --initial-sync-row-batch-size number                       default: 10000                                                                                    
       ZERO_INITIAL_SYNC_ROW_BATCH_SIZE env                                                                                                                       
                                                                The number of rows each table copy worker fetches at a time during                                
                                                                initial sync. This can be increased to speed up initial sync, or decreased                        
                                                                to reduce the amount of heap memory used during initial sync (e.g. for tables                     
                                                                with large rows).                                                                                 
                                                                                                                                                                  
    "
  `);
});
