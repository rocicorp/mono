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
    parseOptions(zeroOptions, {
      argv: ['--help'],
      envNamePrefix: 'ZERO_',
      env: {},
      logger,
      exit,
    }),
  ).toThrow(ExitAfterUsage);
  expect(logger.info).toHaveBeenCalled();
  expect(stripAnsi(logger.info.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --upstream-db string                                          required                                                                                                              
       ZERO_UPSTREAM_DB env                                                                                                                                                              
                                                                   The "upstream" authoritative postgres database.                                                                       
                                                                   In the future we will support other types of upstream besides PG.                                                     
                                                                                                                                                                                         
     --upstream-max-conns number                                   default: 20                                                                                                           
       ZERO_UPSTREAM_MAX_CONNS env                                                                                                                                                       
                                                                   The maximum number of connections to open to the upstream database                                                    
                                                                   for committing mutations. This is divided evenly amongst sync workers.                                                
                                                                   In addition to this number, zero-cache uses one connection for the                                                    
                                                                   replication stream.                                                                                                   
                                                                                                                                                                                         
                                                                   Note that this number must allow for at least one connection per                                                      
                                                                   sync worker, or zero-cache will fail to start. See num-sync-workers                                                   
                                                                                                                                                                                         
     --mutate-url string[]                                         optional                                                                                                              
       ZERO_MUTATE_URL env                                                                                                                                                               
                                                                   The URL of the API server to which zero-cache will push mutations.                                                    
                                                                                                                                                                                         
                                                                   IMPORTANT: URLs are matched using URLPattern, a standard Web API.                                                     
                                                                                                                                                                                         
                                                                   Pattern Syntax:                                                                                                       
                                                                     URLPattern uses a simple and intuitive syntax similar to Express routes.                                            
                                                                     Wildcards and named parameters make it easy to match multiple URLs.                                                 
                                                                                                                                                                                         
                                                                   Basic Examples:                                                                                                       
                                                                     Exact URL match:                                                                                                    
                                                                       "https://api.example.com/mutate"                                                                                  
                                                                     Any subdomain using wildcard:                                                                                       
                                                                       "https://*.example.com/mutate"                                                                                    
                                                                     Multiple subdomain levels:                                                                                          
                                                                       "https://*.*.example.com/mutate"                                                                                  
                                                                     Any path under a domain:                                                                                            
                                                                       "https://api.example.com/*"                                                                                       
                                                                     Named path parameters:                                                                                              
                                                                       "https://api.example.com/:version/mutate"                                                                         
                                                                       ↳ Matches "https://api.example.com/v1/mutate", "https://api.example.com/v2/mutate", etc.                          
                                                                                                                                                                                         
                                                                   Advanced Patterns:                                                                                                    
                                                                     Optional path segments:                                                                                             
                                                                       "https://api.example.com/:path?"                                                                                  
                                                                     Regex in segments (for specific patterns):                                                                          
                                                                       "https://api.example.com/:version(v\\d+)/mutate"                                                                   
                                                                       ↳ Matches only "v" followed by digits                                                                             
                                                                                                                                                                                         
                                                                   Multiple patterns:                                                                                                    
                                                                     ["https://api1.example.com/mutate", "https://api2.example.com/mutate"]                                              
                                                                                                                                                                                         
                                                                   Note: Query parameters and URL fragments (#) are automatically ignored during matching.                               
                                                                                                                                                                                         
                                                                   For full URLPattern syntax, see: https://developer.mozilla.org/en-US/docs/Web/API/URLPattern                          
                                                                                                                                                                                         
     --mutate-api-key string                                       optional                                                                                                              
       ZERO_MUTATE_API_KEY env                                                                                                                                                           
                                                                   An optional secret used to authorize zero-cache to call the API server handling writes.                               
                                                                                                                                                                                         
     --mutate-forward-cookies boolean                              default: false                                                                                                        
       ZERO_MUTATE_FORWARD_COOKIES env                                                                                                                                                   
                                                                   If true, zero-cache will forward cookies from the request.                                                            
                                                                   This is useful for passing authentication cookies to the API server.                                                  
                                                                   If false, cookies are not forwarded.                                                                                  
                                                                                                                                                                                         
     --query-url string[]                                          optional                                                                                                              
       ZERO_QUERY_URL env                                                                                                                                                                
                                                                   The URL of the API server to which zero-cache will send synced queries.                                               
                                                                                                                                                                                         
                                                                   IMPORTANT: URLs are matched using URLPattern, a standard Web API.                                                     
                                                                                                                                                                                         
                                                                   Pattern Syntax:                                                                                                       
                                                                     URLPattern uses a simple and intuitive syntax similar to Express routes.                                            
                                                                     Wildcards and named parameters make it easy to match multiple URLs.                                                 
                                                                                                                                                                                         
                                                                   Basic Examples:                                                                                                       
                                                                     Exact URL match:                                                                                                    
                                                                       "https://api.example.com/mutate"                                                                                  
                                                                     Any subdomain using wildcard:                                                                                       
                                                                       "https://*.example.com/mutate"                                                                                    
                                                                     Multiple subdomain levels:                                                                                          
                                                                       "https://*.*.example.com/mutate"                                                                                  
                                                                     Any path under a domain:                                                                                            
                                                                       "https://api.example.com/*"                                                                                       
                                                                     Named path parameters:                                                                                              
                                                                       "https://api.example.com/:version/mutate"                                                                         
                                                                       ↳ Matches "https://api.example.com/v1/mutate", "https://api.example.com/v2/mutate", etc.                          
                                                                                                                                                                                         
                                                                   Advanced Patterns:                                                                                                    
                                                                     Optional path segments:                                                                                             
                                                                       "https://api.example.com/:path?"                                                                                  
                                                                     Regex in segments (for specific patterns):                                                                          
                                                                       "https://api.example.com/:version(v\\d+)/mutate"                                                                   
                                                                       ↳ Matches only "v" followed by digits                                                                             
                                                                                                                                                                                         
                                                                   Multiple patterns:                                                                                                    
                                                                     ["https://api1.example.com/mutate", "https://api2.example.com/mutate"]                                              
                                                                                                                                                                                         
                                                                   Note: Query parameters and URL fragments (#) are automatically ignored during matching.                               
                                                                                                                                                                                         
                                                                   For full URLPattern syntax, see: https://developer.mozilla.org/en-US/docs/Web/API/URLPattern                          
                                                                                                                                                                                         
     --query-api-key string                                        optional                                                                                                              
       ZERO_QUERY_API_KEY env                                                                                                                                                            
                                                                   An optional secret used to authorize zero-cache to call the API server handling writes.                               
                                                                                                                                                                                         
     --query-forward-cookies boolean                               default: false                                                                                                        
       ZERO_QUERY_FORWARD_COOKIES env                                                                                                                                                    
                                                                   If true, zero-cache will forward cookies from the request.                                                            
                                                                   This is useful for passing authentication cookies to the API server.                                                  
                                                                   If false, cookies are not forwarded.                                                                                  
                                                                                                                                                                                         
     --cvr-db string                                               optional                                                                                                              
       ZERO_CVR_DB env                                                                                                                                                                   
                                                                   The Postgres database used to store CVRs. CVRs (client view records) keep track                                       
                                                                   of the data synced to clients in order to determine the diff to send on reconnect.                                    
                                                                   If unspecified, the upstream-db will be used.                                                                         
                                                                                                                                                                                         
     --cvr-max-conns number                                        default: 30                                                                                                           
       ZERO_CVR_MAX_CONNS env                                                                                                                                                            
                                                                   The maximum number of connections to open to the CVR database.                                                        
                                                                   This is divided evenly amongst sync workers.                                                                          
                                                                                                                                                                                         
                                                                   Note that this number must allow for at least one connection per                                                      
                                                                   sync worker, or zero-cache will fail to start. See num-sync-workers                                                   
                                                                                                                                                                                         
     --cvr-garbage-collection-inactivity-threshold-hours number    default: 48                                                                                                           
       ZERO_CVR_GARBAGE_COLLECTION_INACTIVITY_THRESHOLD_HOURS env                                                                                                                        
                                                                   The duration after which an inactive CVR is eligible for garbage collection.                                          
                                                                   Note that garbage collection is an incremental, periodic process which does not                                       
                                                                   necessarily purge all eligible CVRs immediately.                                                                      
                                                                                                                                                                                         
     --cvr-garbage-collection-initial-interval-seconds number      default: 60                                                                                                           
       ZERO_CVR_GARBAGE_COLLECTION_INITIAL_INTERVAL_SECONDS env                                                                                                                          
                                                                   The initial interval at which to check and garbage collect inactive CVRs.                                             
                                                                   This interval is increased exponentially (up to 16 minutes) when there is                                             
                                                                   nothing to purge.                                                                                                     
                                                                                                                                                                                         
     --cvr-garbage-collection-initial-batch-size number            default: 25                                                                                                           
       ZERO_CVR_GARBAGE_COLLECTION_INITIAL_BATCH_SIZE env                                                                                                                                
                                                                   The initial number of CVRs to purge per garbage collection interval.                                                  
                                                                   This number is increased linearly if the rate of new CVRs exceeds the rate of                                         
                                                                   purged CVRs, in order to reach a steady state.                                                                        
                                                                                                                                                                                         
                                                                   Setting this to 0 effectively disables CVR garbage collection.                                                        
                                                                                                                                                                                         
     --query-hydration-stats boolean                               optional                                                                                                              
       ZERO_QUERY_HYDRATION_STATS env                                                                                                                                                    
                                                                   Track and log the number of rows considered by query hydrations which                                                 
                                                                   take longer than log-slow-hydrate-threshold milliseconds.                                                             
                                                                   This is useful for debugging and performance tuning.                                                                  
                                                                                                                                                                                         
     --enable-query-planner boolean                                default: true                                                                                                         
       ZERO_ENABLE_QUERY_PLANNER env                                                                                                                                                     
                                                                   Enable the query planner for optimizing ZQL queries.                                                                  
                                                                                                                                                                                         
                                                                   The query planner analyzes and optimizes query execution by determining                                               
                                                                   the most efficient join strategies.                                                                                   
                                                                                                                                                                                         
                                                                   You can disable the planner if it is picking bad strategies.                                                          
                                                                                                                                                                                         
     --yield-threshold-ms number                                   default: 10                                                                                                           
       ZERO_YIELD_THRESHOLD_MS env                                                                                                                                                       
                                                                   The maximum amount of time in milliseconds that a sync worker will                                                    
                                                                   spend in IVM (processing query hydration and advancement) before yielding                                             
                                                                   to the event loop. Lower values increase responsiveness and fairness at                                               
                                                                   the cost of reduced throughput.                                                                                       
                                                                                                                                                                                         
     --change-db string                                            optional                                                                                                              
       ZERO_CHANGE_DB env                                                                                                                                                                
                                                                   The Postgres database used to store recent replication log entries, in order                                          
                                                                   to sync multiple view-syncers without requiring multiple replication slots on                                         
                                                                   the upstream database. If unspecified, the upstream-db will be used.                                                  
                                                                                                                                                                                         
     --change-max-conns number                                     default: 5                                                                                                            
       ZERO_CHANGE_MAX_CONNS env                                                                                                                                                         
                                                                   The maximum number of connections to open to the change database.                                                     
                                                                   This is used by the change-streamer for catching up                                                                   
                                                                   zero-cache replication subscriptions.                                                                                 
                                                                                                                                                                                         
     --replica-file string                                         default: "zero.db"                                                                                                    
       ZERO_REPLICA_FILE env                                                                                                                                                             
                                                                   File path to the SQLite replica that zero-cache maintains.                                                            
                                                                   This can be lost, but if it is, zero-cache will have to re-replicate next                                             
                                                                   time it starts up.                                                                                                    
                                                                                                                                                                                         
     --replica-vacuum-interval-hours number                        optional                                                                                                              
       ZERO_REPLICA_VACUUM_INTERVAL_HOURS env                                                                                                                                            
                                                                   Performs a VACUUM at server startup if the specified number of hours has elapsed                                      
                                                                   since the last VACUUM (or initial-sync). The VACUUM operation is heavyweight                                          
                                                                   and requires double the size of the db in disk space. If unspecified, VACUUM                                          
                                                                   operations are not performed.                                                                                         
                                                                                                                                                                                         
     --replica-page-cache-size-kib number                          optional                                                                                                              
       ZERO_REPLICA_PAGE_CACHE_SIZE_KIB env                                                                                                                                              
                                                                   The SQLite page cache size in kibibytes (KiB) for view-syncer connections.                                            
                                                                   The page cache stores recently accessed database pages in memory to reduce disk I/O.                                  
                                                                   Larger cache sizes improve performance for workloads that fit in cache.                                               
                                                                   If unspecified, SQLite's default (~2 MB) is used.                                                                     
                                                                   Note that the effective memory use of this setting will be:                                                           
                                                                   2 * cache_size * num_cores as each connection to the replica gets its own cache                                       
                                                                   and each core maintains 2 connections.                                                                                
                                                                                                                                                                                         
     --log-level debug,info,warn,error                             default: "info"                                                                                                       
       ZERO_LOG_LEVEL env                                                                                                                                                                
                                                                                                                                                                                         
     --log-format text,json                                        default: "text"                                                                                                       
       ZERO_LOG_FORMAT env                                                                                                                                                               
                                                                   Use text for developer-friendly console logging                                                                       
                                                                   and json for consumption by structured-logging services                                                               
                                                                                                                                                                                         
     --log-slow-row-threshold number                               default: 2                                                                                                            
       ZERO_LOG_SLOW_ROW_THRESHOLD env                                                                                                                                                   
                                                                   The number of ms a row must take to fetch from table-source before it is considered slow.                             
                                                                                                                                                                                         
     --log-slow-hydrate-threshold number                           default: 100                                                                                                          
       ZERO_LOG_SLOW_HYDRATE_THRESHOLD env                                                                                                                                               
                                                                   The number of milliseconds a query hydration must take to print a slow warning.                                       
                                                                                                                                                                                         
     --log-ivm-sampling number                                     default: 5000                                                                                                         
       ZERO_LOG_IVM_SAMPLING env                                                                                                                                                         
                                                                   How often to collect IVM metrics. 1 out of N requests will be sampled where N is this value.                          
                                                                                                                                                                                         
     --app-id string                                               default: "zero"                                                                                                       
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
                                                                                                                                                                                         
     --app-publications string[]                                   default: []                                                                                                           
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
                                                                                                                                                                                         
     --port number                                                 default: 4848                                                                                                         
       ZERO_PORT env                                                                                                                                                                     
                                                                   The port for sync connections.                                                                                        
                                                                                                                                                                                         
     --change-streamer-uri string                                  optional                                                                                                              
       ZERO_CHANGE_STREAMER_URI env                                                                                                                                                      
                                                                   When set, connects to the change-streamer at the given URI.                                                           
                                                                   In a multi-node setup, this should be specified in view-syncer options,                                               
                                                                   pointing to the replication-manager URI, which runs a change-streamer                                                 
                                                                   on port 4849.                                                                                                         
                                                                                                                                                                                         
     --change-streamer-mode dedicated,discover                     default: "dedicated"                                                                                                  
       ZERO_CHANGE_STREAMER_MODE env                                                                                                                                                     
                                                                   As an alternative to ZERO_CHANGE_STREAMER_URI, the ZERO_CHANGE_STREAMER_MODE                                          
                                                                   can be set to "discover" to instruct the view-syncer to connect to the                                                
                                                                   ip address registered by the replication-manager upon startup.                                                        
                                                                                                                                                                                         
                                                                   This may not work in all networking configurations, e.g. certain private                                              
                                                                   networking or port forwarding configurations. Using the ZERO_CHANGE_STREAMER_URI                                      
                                                                   with an explicit routable hostname is recommended instead.                                                            
                                                                                                                                                                                         
                                                                   Note: This option is ignored if the ZERO_CHANGE_STREAMER_URI is set.                                                  
                                                                                                                                                                                         
     --change-streamer-port number                                 optional                                                                                                              
       ZERO_CHANGE_STREAMER_PORT env                                                                                                                                                     
                                                                   The port on which the change-streamer runs. This is an internal                                                       
                                                                   protocol between the replication-manager and view-syncers, which                                                      
                                                                   runs in the same process tree in local development or a single-node configuration.                                    
                                                                                                                                                                                         
                                                                   If unspecified, defaults to --port + 1.                                                                               
                                                                                                                                                                                         
     --change-streamer-startup-delay-ms number                     default: 15000                                                                                                        
       ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS env                                                                                                                                         
                                                                   The delay to wait before the change-streamer takes over the replication stream                                        
                                                                   (i.e. the handoff during replication-manager updates), to allow loadbalancers to register                             
                                                                   the task as healthy based on healthcheck parameters. Note that if a change stream request                             
                                                                   is received during this interval, the delay will be canceled and the takeover will happen                             
                                                                   immediately, since the incoming request indicates that the task is registered as a target.                            
                                                                                                                                                                                         
     --change-streamer-back-pressure-threshold number              default: 100000                                                                                                       
       ZERO_CHANGE_STREAMER_BACK_PRESSURE_THRESHOLD env                                                                                                                                  
                                                                   The maximum number of queued changes before back pressure is applied to the                                           
                                                                   change source. When the queue exceeds this threshold, the change-streamer pauses                                      
                                                                   consumption from upstream until the queue drops to 90% of the threshold.                                              
                                                                                                                                                                                         
                                                                   Increasing this value may improve throughput at the cost of higher memory usage.                                      
                                                                                                                                                                                         
     --task-id string                                              optional                                                                                                              
       ZERO_TASK_ID env                                                                                                                                                                  
                                                                   Globally unique identifier for the zero-cache instance.                                                               
                                                                                                                                                                                         
                                                                   Setting this to a platform specific task identifier can be useful for debugging.                                      
                                                                   If unspecified, zero-cache will attempt to extract the TaskARN if run from within                                     
                                                                   an AWS ECS container, and otherwise use a random string.                                                              
                                                                                                                                                                                         
     --per-user-mutation-limit-max number                          optional                                                                                                              
       ZERO_PER_USER_MUTATION_LIMIT_MAX env                                                                                                                                              
                                                                   The maximum mutations per user within the specified windowMs.                                                         
                                                                   If unset, no rate limiting is enforced.                                                                               
                                                                                                                                                                                         
     --per-user-mutation-limit-window-ms number                    default: 60000                                                                                                        
       ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS env                                                                                                                                        
                                                                   The sliding window over which the perUserMutationLimitMax is enforced.                                                
                                                                                                                                                                                         
     --num-sync-workers number                                     optional                                                                                                              
       ZERO_NUM_SYNC_WORKERS env                                                                                                                                                         
                                                                   The number of processes to use for view syncing.                                                                      
                                                                   Leave this unset to use the maximum available parallelism.                                                            
                                                                   If set to 0, the server runs without sync workers, which is the                                                       
                                                                   configuration for running the replication-manager.                                                                    
                                                                                                                                                                                         
     --auto-reset boolean                                          default: true                                                                                                         
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
                                                                                                                                                                                         
     --admin-password string                                       optional                                                                                                              
       ZERO_ADMIN_PASSWORD env                                                                                                                                                           
                                                                   A password used to administer zero-cache server, for example to access the                                            
                                                                   /statz endpoint.                                                                                                      
                                                                                                                                                                                         
                                                                   A password is optional in development mode but required in production mode.                                           
                                                                                                                                                                                         
     --websocket-compression boolean                               default: false                                                                                                        
       ZERO_WEBSOCKET_COMPRESSION env                                                                                                                                                    
                                                                   Enable WebSocket per-message deflate compression.                                                                     
                                                                                                                                                                                         
                                                                   Compression can reduce bandwidth usage for sync traffic but                                                           
                                                                   increases CPU usage on both client and server. Disabled by default.                                                   
                                                                                                                                                                                         
                                                                   See: https://github.com/websockets/ws#websocket-compression                                                           
                                                                                                                                                                                         
     --websocket-compression-options string                        optional                                                                                                              
       ZERO_WEBSOCKET_COMPRESSION_OPTIONS env                                                                                                                                            
                                                                   JSON string containing WebSocket compression options.                                                                 
                                                                                                                                                                                         
                                                                   Only used if websocketCompression is enabled.                                                                         
                                                                                                                                                                                         
                                                                   Example: {"zlibDeflateOptions":{"level":3},"threshold":1024}                                                          
                                                                                                                                                                                         
                                                                   See https://github.com/websockets/ws/blob/master/doc/ws.md#new-websocketserveroptions-callback for available options. 
                                                                                                                                                                                         
     --litestream-executable string                                optional                                                                                                              
       ZERO_LITESTREAM_EXECUTABLE env                                                                                                                                                    
                                                                   Path to the litestream executable.                                                                                    
                                                                                                                                                                                         
     --litestream-config-path string                               default: "./src/services/litestream/config.yml"                                                                       
       ZERO_LITESTREAM_CONFIG_PATH env                                                                                                                                                   
                                                                   Path to the litestream yaml config file. zero-cache will run this with its                                            
                                                                   environment variables, which can be referenced in the file via \${ENV}                                                 
                                                                   substitution, for example:                                                                                            
                                                                   * ZERO_REPLICA_FILE for the db path                                                                                   
                                                                   * ZERO_LITESTREAM_BACKUP_LOCATION for the db replica url                                                              
                                                                   * ZERO_LITESTREAM_LOG_LEVEL for the log level                                                                         
                                                                   * ZERO_LOG_FORMAT for the log type                                                                                    
                                                                                                                                                                                         
     --litestream-log-level debug,info,warn,error                  default: "warn"                                                                                                       
       ZERO_LITESTREAM_LOG_LEVEL env                                                                                                                                                     
                                                                                                                                                                                         
     --litestream-backup-url string                                optional                                                                                                              
       ZERO_LITESTREAM_BACKUP_URL env                                                                                                                                                    
                                                                   The location of the litestream backup, usually an s3:// URL.                                                          
                                                                   This is only consulted by the replication-manager.                                                                    
                                                                   view-syncers receive this information from the replication-manager.                                                   
                                                                                                                                                                                         
     --litestream-endpoint string                                  optional                                                                                                              
       ZERO_LITESTREAM_ENDPOINT env                                                                                                                                                      
                                                                   The S3-compatible endpoint URL to use for the litestream backup. Only required for non-AWS services.                  
                                                                   This is only consulted by the replication-manager.                                                                    
                                                                                                                                                                                         
     --litestream-port number                                      optional                                                                                                              
       ZERO_LITESTREAM_PORT env                                                                                                                                                          
                                                                   Port on which litestream exports metrics, used to determine the replication                                           
                                                                   watermark up to which it is safe to purge change log records.                                                         
                                                                                                                                                                                         
                                                                   If unspecified, defaults to --port + 2.                                                                               
                                                                                                                                                                                         
     --litestream-checkpoint-threshold-mb number                   default: 40                                                                                                           
       ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB env                                                                                                                                       
                                                                   The size of the WAL file at which to perform an SQlite checkpoint to apply                                            
                                                                   the writes in the WAL to the main database file. Each checkpoint creates                                              
                                                                   a new WAL segment file that will be backed up by litestream. Smaller thresholds                                       
                                                                   may improve read performance, at the expense of creating more files to download                                       
                                                                   when restoring the replica from the backup.                                                                           
                                                                                                                                                                                         
     --litestream-min-checkpoint-page-count number                 optional                                                                                                              
       ZERO_LITESTREAM_MIN_CHECKPOINT_PAGE_COUNT env                                                                                                                                     
                                                                   The WAL page count at which SQLite attempts a PASSIVE checkpoint, which                                               
                                                                   transfers pages to the main database file without blocking writers.                                                   
                                                                   Defaults to checkpointThresholdMB * 250 (since SQLite page size is 4KB).                                              
                                                                                                                                                                                         
     --litestream-max-checkpoint-page-count number                 optional                                                                                                              
       ZERO_LITESTREAM_MAX_CHECKPOINT_PAGE_COUNT env                                                                                                                                     
                                                                   The WAL page count at which SQLite performs a RESTART checkpoint, which                                               
                                                                   blocks writers until complete. Defaults to minCheckpointPageCount * 10.                                               
                                                                   Set to 0 to disable RESTART checkpoints entirely.                                                                     
                                                                                                                                                                                         
     --litestream-incremental-backup-interval-minutes number       default: 15                                                                                                           
       ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES env                                                                                                                           
                                                                   The interval between incremental backups of the replica. Shorter intervals                                            
                                                                   reduce the amount of change history that needs to be replayed when catching                                           
                                                                   up a new view-syncer, at the expense of increasing the number of files needed                                         
                                                                   to download for the initial litestream restore.                                                                       
                                                                                                                                                                                         
     --litestream-snapshot-backup-interval-hours number            default: 12                                                                                                           
       ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_HOURS env                                                                                                                                
                                                                   The interval between snapshot backups of the replica. Snapshot backups                                                
                                                                   make a full copy of the database to a new litestream generation. This                                                 
                                                                   improves restore time at the expense of bandwidth. Applications with a                                                
                                                                   large database and low write rate can increase this interval to reduce                                                
                                                                   network usage for backups (litestream defaults to 24 hours).                                                          
                                                                                                                                                                                         
     --litestream-restore-parallelism number                       default: 48                                                                                                           
       ZERO_LITESTREAM_RESTORE_PARALLELISM env                                                                                                                                           
                                                                   The number of WAL files to download in parallel when performing the                                                   
                                                                   initial restore of the replica from the backup.                                                                       
                                                                                                                                                                                         
     --litestream-multipart-concurrency number                     default: 48                                                                                                           
       ZERO_LITESTREAM_MULTIPART_CONCURRENCY env                                                                                                                                         
                                                                   The number of parts (of size --litestream-multipart-size bytes)                                                       
                                                                   to upload or download in parallel when backing up or restoring the snapshot.                                          
                                                                                                                                                                                         
     --litestream-multipart-size number                            default: 16777216                                                                                                     
       ZERO_LITESTREAM_MULTIPART_SIZE env                                                                                                                                                
                                                                   The size of each part when uploading or downloading the snapshot with                                                 
                                                                   --multipart-concurrency. Note that up to concurrency * size                                                           
                                                                   bytes of memory are used when backing up or restoring the snapshot.                                                   
                                                                                                                                                                                         
     --storage-db-tmp-dir string                                   optional                                                                                                              
       ZERO_STORAGE_DB_TMP_DIR env                                                                                                                                                       
                                                                   tmp directory for IVM operator storage. Leave unset to use os.tmpdir()                                                
                                                                                                                                                                                         
     --initial-sync-table-copy-workers number                      default: 5                                                                                                            
       ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS env                                                                                                                                          
                                                                   The number of parallel workers used to copy tables during initial sync.                                               
                                                                   Each worker uses a database connection and will buffer up to (approximately)                                          
                                                                   10 MB of table data in memory during initial sync. Increasing the number of                                           
                                                                   workers may improve initial sync speed; however, note that local disk throughput                                      
                                                                   (i.e. IOPS), upstream CPU, and network bandwidth may also be bottlenecks.                                             
                                                                                                                                                                                         
     --lazy-startup boolean                                        default: false                                                                                                        
       ZERO_LAZY_STARTUP env                                                                                                                                                             
                                                                   Delay starting the majority of zero-cache until first request.                                                        
                                                                                                                                                                                         
                                                                   This is mainly intended to avoid connecting to Postgres replication stream                                            
                                                                   until the first request is received, which can be useful i.e., for preview instances.                                 
                                                                                                                                                                                         
                                                                   Currently only supported in single-node mode.                                                                         
                                                                                                                                                                                         
     --server-version string                                       optional                                                                                                              
       ZERO_SERVER_VERSION env                                                                                                                                                           
                                                                   The version string outputted to logs when the server starts up.                                                       
                                                                                                                                                                                         
     --enable-telemetry boolean                                    default: true                                                                                                         
       ZERO_ENABLE_TELEMETRY env                                                                                                                                                         
                                                                   Set to false to opt out of telemetry collection.                                                                      
                                                                                                                                                                                         
                                                                   This helps us improve Zero by collecting anonymous usage data.                                                        
                                                                   Setting the DO_NOT_TRACK environment variable also disables telemetry.                                                
                                                                                                                                                                                         
     --cloud-event-sink-env string                                 optional                                                                                                              
       ZERO_CLOUD_EVENT_SINK_ENV env                                                                                                                                                     
                                                                   ENV variable containing a URI to a CloudEvents sink. When set, ZeroEvents                                             
                                                                   will be published to the sink as the data field of CloudEvents.                                                       
                                                                   The source field of the CloudEvents will be set to the ZERO_TASK_ID,                                                  
                                                                   along with any extension attributes specified by the ZERO_CLOUD_EVENT_EXTENSION_OVERRIDES_ENV.                        
                                                                                                                                                                                         
                                                                   This configuration is modeled to easily integrate with a knative K_SINK binding,                                      
                                                                   (i.e. https://github.com/knative/eventing/blob/main/docs/spec/sources.md#sinkbinding).                                
                                                                   However, any CloudEvents sink can be used.                                                                            
                                                                                                                                                                                         
     --cloud-event-extension-overrides-env string                  optional                                                                                                              
       ZERO_CLOUD_EVENT_EXTENSION_OVERRIDES_ENV env                                                                                                                                      
                                                                   ENV variable containing a JSON stringified object with an extensions field                                            
                                                                   containing attributes that should be added or overridden on outbound CloudEvents.                                     
                                                                                                                                                                                         
                                                                   This configuration is modeled to easily integrate with a knative K_CE_OVERRIDES binding,                              
                                                                   (i.e. https://github.com/knative/eventing/blob/main/docs/spec/sources.md#sinkbinding).                                
                                                                                                                                                                                         
    "
  `);
});

test.each([['has/slashes'], ['has-dashes'], ['has.dots']])(
  '--app-id %s',
  appID => {
    const logger = {info: vi.fn()};
    expect(() =>
      parseOptionsAdvanced(zeroOptions, {
        argv: ['--app-id', appID],
        envNamePrefix: 'ZERO_',
        allowUnknown: false,
        allowPartial: true,
        env: {},
        logger,
        exit,
      }),
    ).toThrowError(INVALID_APP_ID_MESSAGE);
  },
);

test.each([['isok'], ['has_underscores'], ['1'], ['123']])(
  '--app-id %s',
  appID => {
    const {config} = parseOptionsAdvanced(zeroOptions, {
      argv: ['--app-id', appID],
      envNamePrefix: 'ZERO_',
      allowUnknown: false,
      allowPartial: true,
    });
    expect(config.app.id).toBe(appID);
  },
);

test('--shard-id disallowed', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptionsAdvanced(zeroOptions, {
      argv: ['--shard-id', 'prod'],
      envNamePrefix: 'ZERO_',
      allowUnknown: false,
      allowPartial: true,
      env: {},
      logger,
      exit,
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: ZERO_SHARD_ID is no longer an option. Please use ZERO_APP_ID instead.]`,
  );
});
