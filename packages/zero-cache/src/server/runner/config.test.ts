import {stripVTControlCharacters as stripAnsi} from 'node:util';
import {expect, test, vi} from 'vitest';
import {parseOptions} from '../../../../shared/src/options.ts';
import {getMultiZeroConfig, multiConfigSchema} from './config.ts';

test('parse options', () => {
  expect(
    getMultiZeroConfig(
      {
        ['ZERO_UPSTREAM_DB']: 'foo',
      },
      [
        '--tenants-json',
        JSON.stringify({
          tenants: [
            {
              id: 'ten-boo',
              host: 'Normalize.ME',
              path: 'tenboo',
              env: {
                ['ZERO_REPLICA_FILE']: 'tenboo.db',
                ['ZERO_CVR_DB']: 'foo',
                ['ZERO_CHANGE_DB']: 'foo',
                ['ZERO_APP_ID']: 'foo',
              },
            },
            {
              id: 'ten_bar',
              path: '/tenbar',
              env: {
                ['ZERO_REPLICA_FILE']: 'tenbar.db',
                ['ZERO_CVR_DB']: 'bar',
                ['ZERO_CHANGE_DB']: 'bar',
                ['ZERO_APP_ID']: 'bar',
              },
            },
            {
              id: 'tenbaz-123',
              path: '/tenbaz',
              env: {
                ['ZERO_REPLICA_FILE']: 'tenbar.db',
                ['ZERO_UPSTREAM_DB']: 'overridden',
                ['ZERO_CVR_DB']: 'baz',
                ['ZERO_CHANGE_DB']: 'baz',
                ['ZERO_APP_ID']: 'foo',
              },
            },
          ],
        }),
      ],
    ),
  ).toMatchInlineSnapshot(`
    {
      "config": {
        "app": {
          "id": "zero",
          "publications": [],
        },
        "auth": {},
        "autoReset": true,
        "change": {
          "maxConns": 5,
        },
        "cvr": {
          "maxConns": 30,
        },
        "initialSync": {
          "rowBatchSize": 10000,
          "tableCopyWorkers": 5,
        },
        "litestream": {
          "checkpointThresholdMB": 40,
          "configPath": "./src/services/litestream/config.yml",
          "incrementalBackupIntervalMinutes": 15,
          "logLevel": "warn",
          "restoreParallelism": 48,
          "snapshotBackupIntervalHours": 12,
        },
        "log": {
          "format": "text",
          "ivmSampling": 5000,
          "level": "info",
          "slowHydrateThreshold": 100,
          "slowRowThreshold": 2,
        },
        "perUserMutationLimit": {
          "windowMs": 60000,
        },
        "port": 4848,
        "push": {},
        "replica": {},
        "run": {
          "lazily": false,
        },
        "shard": {
          "num": 0,
        },
        "targetClientRowCount": 20000,
        "tenants": [
          {
            "env": {
              "ZERO_APP_ID": "foo",
              "ZERO_CHANGE_DB": "foo",
              "ZERO_CVR_DB": "foo",
              "ZERO_REPLICA_FILE": "tenboo.db",
            },
            "host": "normalize.me",
            "id": "ten-boo",
            "path": "/tenboo/",
          },
          {
            "env": {
              "ZERO_APP_ID": "bar",
              "ZERO_CHANGE_DB": "bar",
              "ZERO_CVR_DB": "bar",
              "ZERO_REPLICA_FILE": "tenbar.db",
            },
            "id": "ten_bar",
            "path": "/tenbar/",
          },
          {
            "env": {
              "ZERO_APP_ID": "foo",
              "ZERO_CHANGE_DB": "baz",
              "ZERO_CVR_DB": "baz",
              "ZERO_REPLICA_FILE": "tenbar.db",
              "ZERO_UPSTREAM_DB": "overridden",
            },
            "id": "tenbaz-123",
            "path": "/tenbaz/",
          },
        ],
        "upstream": {
          "db": "foo",
          "maxConns": 20,
          "type": "pg",
        },
      },
      "env": {
        "ZERO_APP_ID": "zero",
        "ZERO_APP_PUBLICATIONS": "",
        "ZERO_AUTO_RESET": "true",
        "ZERO_CHANGE_MAX_CONNS": "5",
        "ZERO_CVR_MAX_CONNS": "30",
        "ZERO_INITIAL_SYNC_ROW_BATCH_SIZE": "10000",
        "ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS": "5",
        "ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB": "40",
        "ZERO_LITESTREAM_CONFIG_PATH": "./src/services/litestream/config.yml",
        "ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES": "15",
        "ZERO_LITESTREAM_LOG_LEVEL": "warn",
        "ZERO_LITESTREAM_RESTORE_PARALLELISM": "48",
        "ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_HOURS": "12",
        "ZERO_LOG_FORMAT": "text",
        "ZERO_LOG_IVM_SAMPLING": "5000",
        "ZERO_LOG_LEVEL": "info",
        "ZERO_LOG_SLOW_HYDRATE_THRESHOLD": "100",
        "ZERO_LOG_SLOW_ROW_THRESHOLD": "2",
        "ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS": "60000",
        "ZERO_PORT": "4848",
        "ZERO_RUN_LAZILY": "false",
        "ZERO_SHARD_NUM": "0",
        "ZERO_TARGET_CLIENT_ROW_COUNT": "20000",
        "ZERO_TENANTS_JSON": "{"tenants":[{"id":"ten-boo","host":"Normalize.ME","path":"tenboo","env":{"ZERO_REPLICA_FILE":"tenboo.db","ZERO_CVR_DB":"foo","ZERO_CHANGE_DB":"foo","ZERO_APP_ID":"foo"}},{"id":"ten_bar","path":"/tenbar","env":{"ZERO_REPLICA_FILE":"tenbar.db","ZERO_CVR_DB":"bar","ZERO_CHANGE_DB":"bar","ZERO_APP_ID":"bar"}},{"id":"tenbaz-123","path":"/tenbaz","env":{"ZERO_REPLICA_FILE":"tenbar.db","ZERO_UPSTREAM_DB":"overridden","ZERO_CVR_DB":"baz","ZERO_CHANGE_DB":"baz","ZERO_APP_ID":"foo"}}]}",
        "ZERO_UPSTREAM_DB": "foo",
        "ZERO_UPSTREAM_MAX_CONNS": "20",
        "ZERO_UPSTREAM_TYPE": "pg",
      },
    }
  `);
});

test.each([
  [
    'Only a single path component may be specified',
    [
      {
        id: 'tenboo',
        path: '/too/many-slashes',
        env: {
          ['ZERO_REPLICA_FILE']: 'foo.db',
          ['ZERO_CVR_DB']: 'foo',
          ['ZERO_CHANGE_DB']: 'foo',
        },
      },
    ],
  ],
  [
    'Unexpected property ZERO_UPSTREAM_DBZ',
    [
      {
        id: 'tenboo',
        path: '/zero',
        env: {
          ['ZERO_UPSTREAM_DBZ']: 'oops',
          ['ZERO_REPLICA_FILE']: 'boo.db',
          ['ZERO_CVR_DB']: 'boo',
          ['ZERO_CHANGE_DB']: 'boo',
        },
      },
    ],
  ],
  [
    'Must be non-empty',
    [
      {
        id: '',
        path: '/foo',
        env: {
          ['ZERO_REPLICA_FILE']: 'foo.db',
          ['ZERO_CVR_DB']: 'foo',
          ['ZERO_CHANGE_DB']: 'foo',
        },
      },
    ],
  ],
  [
    'contain only alphanumeric characters, underscores, and hyphens',
    [
      {
        id: 'id/with/slashes',
        path: '/foo',
        env: {
          ['ZERO_REPLICA_FILE']: 'foo.db',
          ['ZERO_CVR_DB']: 'foo',
          ['ZERO_CHANGE_DB']: 'foo',
        },
      },
    ],
  ],
  [
    'Multiple tenants with ID',
    [
      {
        id: 'foo',
        path: '/foo',
        env: {
          ['ZERO_REPLICA_FILE']: 'foo.db',
          ['ZERO_CVR_DB']: 'foo',
          ['ZERO_CHANGE_DB']: 'foo',
        },
      },
      {
        id: 'foo',
        path: '/bar',
        env: {
          ['ZERO_REPLICA_FILE']: 'bar.db',
          ['ZERO_CVR_DB']: 'bar',
          ['ZERO_CHANGE_DB']: 'bar',
        },
      },
    ],
  ],
])('%s', (errMsg, tenants) => {
  expect(() =>
    getMultiZeroConfig({}, ['--tenants-json', JSON.stringify({tenants})]),
  ).toThrowError(errMsg);
});

class ExitAfterUsage extends Error {}
const exit = () => {
  throw new ExitAfterUsage();
};

// Tip: Rerun tests with -u to update the snapshot.
test('zero-cache --help', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptions(multiConfigSchema, ['--help'], 'ZERO_', {}, logger, exit),
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
                                                                                                                                                                  
     --cvr-db string                                            optional                                                                                          
       ZERO_CVR_DB env                                                                                                                                            
                                                                The Postgres database used to store CVRs. CVRs (client view records) keep track                   
                                                                of the data synced to clients in order to determine the diff to send on reconnect.                
                                                                If unspecified, the upstream-db will be used.                                                     
                                                                                                                                                                  
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
                                                                                                                                                                  
     --change-db string                                         optional                                                                                          
       ZERO_CHANGE_DB env                                                                                                                                         
                                                                The Postgres database used to store recent replication log entries, in order                      
                                                                to sync multiple view-syncers without requiring multiple replication slots on                     
                                                                the upstream database. If unspecified, the upstream-db will be used.                              
                                                                                                                                                                  
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
                                                                                                                                                                  
     --replica-vacuum-interval-hours number                     optional                                                                                          
       ZERO_REPLICA_VACUUM_INTERVAL_HOURS env                                                                                                                     
                                                                Performs a VACUUM at server startup if the specified number of hours has elapsed                  
                                                                since the last VACUUM (or initial-sync). The VACUUM operation is heavyweight                      
                                                                and requires double the size of the db in disk space. If unspecified, VACUUM                      
                                                                operations are not performed.                                                                     
                                                                                                                                                                  
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
                                                                                                                                                                  
     --log-slow-hydrate-threshold number                        default: 100                                                                                      
       ZERO_LOG_SLOW_HYDRATE_THRESHOLD env                                                                                                                        
                                                                The number of milliseconds a query hydration must take to print a slow warning.                   
                                                                                                                                                                  
     --log-ivm-sampling number                                  default: 5000                                                                                     
       ZERO_LOG_IVM_SAMPLING env                                                                                                                                  
                                                                How often to collect IVM metrics. 1 out of N requests will be sampled where N is this value.      
                                                                                                                                                                  
     --app-id string                                            default: "zero"                                                                                   
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
                                                                                                                                                                  
     --app-publications string[]                                default: []                                                                                       
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
                                                                                                                                                                  
     --target-client-row-count number                           default: 20000                                                                                    
       ZERO_TARGET_CLIENT_ROW_COUNT env                                                                                                                           
                                                                The target number of rows to keep per client in the client side cache.                            
                                                                This limit is a soft limit. When the number of rows in the cache exceeds                          
                                                                this limit, zero-cache will evict inactive queries in order of ttl-based expiration.              
                                                                Active queries, on the other hand, are never evicted and are allowed to use more                  
                                                                rows than the limit.                                                                              
                                                                                                                                                                  
     --run-lazily boolean                                       default: false                                                                                    
       ZERO_RUN_LAZILY env                                                                                                                                        
                                                                Delay starting the zero-cache processes until the first request.                                  
                                                                                                                                                                  
                                                                Note: This works as expected in single-node mode. While it is technically usable                  
                                                                in a multi-node setup, there is a bootstrapping complication in that the                          
                                                                view-syncer must first restore the replica from litestream before connecting to                   
                                                                the replication-manager (to know where to continue replication from). If the                      
                                                                replication-manager has never run, there will be no replica file to restore, and                  
                                                                the view-syncer will fail to start up, never connecting to the replication-manager.               
                                                                As such, it is not recommended to run a replication-manager lazily.                               
                                                                                                                                                                  
     --server-version string                                    optional                                                                                          
       ZERO_SERVER_VERSION env                                                                                                                                    
                                                                The version string outputted to logs when the server starts up.                                   
                                                                                                                                                                  
     --tenants-json string                                      optional                                                                                          
       ZERO_TENANTS_JSON env                                                                                                                                      
                                                                JSON encoding of per-tenant configs for running the server in multi-tenant mode:                  
                                                                                                                                                                  
                                                                {                                                                                                 
                                                                  /**                                                                                             
                                                                   * Requests to the main application port are dispatched to the first tenant                     
                                                                   * with a matching host and path. If both host and path are specified,                          
                                                                   * both must match for the request to be dispatched to that tenant.                             
                                                                   *                                                                                              
                                                                   * Requests can also be sent directly to the ZERO_PORT specified                                
                                                                   * in a tenant's env overrides. In this case, no host or path                                   
                                                                   * matching is necessary.                                                                       
                                                                   */                                                                                             
                                                                  tenants: {                                                                                      
                                                                     /**                                                                                          
                                                                      * Unique per-tenant ID used internally for multi-node dispatch.                             
                                                                      *                                                                                           
                                                                      * The ID may only contain alphanumeric characters, underscores, and hyphens.                
                                                                      * Note that changing the ID may result in temporary disruption in multi-node                
                                                                      * mode, when the configs in the view-syncer and replication-manager differ.                 
                                                                      */                                                                                          
                                                                     id: string;                                                                                  
                                                                     host?: string;  // case-insensitive full Host: header match                                  
                                                                     path?: string;  // first path component, with or without leading slash                       
                                                                                                                                                                  
                                                                     /**                                                                                          
                                                                      * Options are inherited from the main application (e.g. args and ENV) by default,           
                                                                      * and are overridden by values in the tenant's env object.                                  
                                                                      */                                                                                          
                                                                     env: {                                                                                       
                                                                       ZERO_REPLICA_FILE: string                                                                  
                                                                       ZERO_UPSTREAM_DB: string                                                                   
                                                                       ZERO_CVR_DB: string                                                                        
                                                                       ZERO_CHANGE_DB: string                                                                     
                                                                       ...                                                                                        
                                                                     };                                                                                           
                                                                  }[];                                                                                            
                                                                }                                                                                                 
                                                                                                                                                                  
    "
  `);
});
