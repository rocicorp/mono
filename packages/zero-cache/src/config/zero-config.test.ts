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
     --upstream-db string                                                          required                                                                                                                   
       ZERO_UPSTREAM_DB env                                                                                                                                                                                   
                                                                                   The "upstream" authoritative postgres database.                                                                            
                                                                                   In the future we will support other types of upstream besides PG.                                                          
                                                                                                                                                                                                              
     --upstream-max-conns number                                                   default: 20                                                                                                                
       ZERO_UPSTREAM_MAX_CONNS env                                                                                                                                                                            
                                                                                   The maximum number of connections to open to the upstream database                                                         
                                                                                   for committing mutations. This is divided evenly amongst sync workers.                                                     
                                                                                   In addition to this number, zero-cache uses one connection for the                                                         
                                                                                   replication stream.                                                                                                        
                                                                                                                                                                                                              
                                                                                   Note that this number must allow for at least one connection per                                                           
                                                                                   sync worker, or zero-cache will fail to start. See num-sync-workers                                                        
                                                                                                                                                                                                              
     --upstream-pg-replication-slot-failover boolean                               optional                                                                                                                   
       ZERO_UPSTREAM_PG_REPLICATION_SLOT_FAILOVER env                                                                                                                                                         
                                                                                   For upstream Postgres versions 17+, creates replication slots with the                                                     
                                                                                   failover parameter set to true to enable slot synchronization                                                              
                                                                                   and failover. Note that additional Postgres-level configuration is necessary                                               
                                                                                   when enabling this option. For details, see:                                                                               
                                                                                                                                                                                                              
                                                                                   https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION 
                                                                                                                                                                                                              
                                                                                   (Note that this option has no effect for Postgres versions before 17.)                                                     
                                                                                                                                                                                                              
     --upstream-pg-stream-inbound-timeout-ms number                                optional                                                                                                                   
       ZERO_UPSTREAM_PG_STREAM_INBOUND_TIMEOUT_MS env                                                                                                                                                         
                                                                                   The time (in milliseconds) without any inbound message from the upstream                                                   
                                                                                   wal sender after which the replication stream is considered unresponsive                                                   
                                                                                   and torn down to force a reconnect.                                                                                        
                                                                                                                                                                                                              
                                                                                   Defaults to 2x the server's wal_sender_timeout. That suits idle                                                            
                                                                                   streams, but a busy wal sender can be legitimately silent for longer —                                                     
                                                                                   e.g. while decoding through WAL from unpublished tables, or assembling a                                                   
                                                                                   large transaction that is only sent at commit. If the server's                                                             
                                                                                   wal_sender_timeout is aggressive (some managed environments                                                                
                                                                                   default it as low as 5 seconds), the resulting teardown aborts and                                                         
                                                                                   replays the in-flight transaction, which can prevent replication from                                                      
                                                                                   ever catching up on a large backlog. Set this option to widen the                                                          
                                                                                   client-side threshold without changing the server setting.                                                                 
                                                                                                                                                                                                              
                                                                                   (This option has no effect when wal_sender_timeout is 0, which                                                             
                                                                                   disables inbound liveness detection entirely.)                                                                             
                                                                                                                                                                                                              
     --mutate-url string[]                                                         optional                                                                                                                   
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
                                                                                                                                                                                                              
     --mutate-api-key string                                                       optional                                                                                                                   
       ZERO_MUTATE_API_KEY env                                                                                                                                                                                
                                                                                   An optional secret used to authorize zero-cache to call the API server handling writes.                                    
                                                                                                                                                                                                              
     --mutate-forward-cookies boolean                                              default: false                                                                                                             
       ZERO_MUTATE_FORWARD_COOKIES env                                                                                                                                                                        
                                                                                   If true, zero-cache will forward cookies from the request.                                                                 
                                                                                   This is useful for passing authentication cookies to the API server.                                                       
                                                                                   If false, cookies are not forwarded.                                                                                       
                                                                                                                                                                                                              
     --mutate-allowed-client-headers string[]                                      optional                                                                                                                   
       ZERO_MUTATE_ALLOWED_CLIENT_HEADERS env                                                                                                                                                                 
                                                                                   A list of header names that clients are allowed to set via custom headers.                                                 
                                                                                   If specified, only headers in this list will be forwarded to the push URL.                                                 
                                                                                   Header names are case-insensitive.                                                                                         
                                                                                   If not specified, no client-provided headers are forwarded.                                                                
                                                                                   Example: ZERO_MUTATE_ALLOWED_CLIENT_HEADERS=x-request-id,x-correlation-id                                                  
                                                                                                                                                                                                              
     --mutate-allowed-request-headers string[]                                     optional                                                                                                                   
       ZERO_MUTATE_ALLOWED_REQUEST_HEADERS env                                                                                                                                                                
                                                                                   A list of header names to forward from the incoming HTTP request to the push URL.                                          
                                                                                   Unlike allowed-client-headers (which forwards headers set by the client), these are taken                                  
                                                                                   from the HTTP request that established the connection (e.g. headers injected by a proxy or load balancer).                 
                                                                                   If a listed header is present on the request, its value is forwarded upstream under the same header name.                  
                                                                                   Header names are case-insensitive.                                                                                         
                                                                                   If not specified, no request headers are forwarded.                                                                        
                                                                                   Example: ZERO_MUTATE_ALLOWED_REQUEST_HEADERS=x-forwarded-for,cf-ray                                                        
                                                                                                                                                                                                              
     --query-url string[]                                                          optional                                                                                                                   
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
                                                                                                                                                                                                              
     --query-api-key string                                                        optional                                                                                                                   
       ZERO_QUERY_API_KEY env                                                                                                                                                                                 
                                                                                   An optional secret used to authorize zero-cache to call the API server handling writes.                                    
                                                                                                                                                                                                              
     --query-forward-cookies boolean                                               default: false                                                                                                             
       ZERO_QUERY_FORWARD_COOKIES env                                                                                                                                                                         
                                                                                   If true, zero-cache will forward cookies from the request.                                                                 
                                                                                   This is useful for passing authentication cookies to the API server.                                                       
                                                                                   If false, cookies are not forwarded.                                                                                       
                                                                                                                                                                                                              
     --query-allowed-client-headers string[]                                       optional                                                                                                                   
       ZERO_QUERY_ALLOWED_CLIENT_HEADERS env                                                                                                                                                                  
                                                                                   A list of header names that clients are allowed to set via custom headers.                                                 
                                                                                   If specified, only headers in this list will be forwarded to the query URL.                                                
                                                                                   Header names are case-insensitive.                                                                                         
                                                                                   If not specified, no client-provided headers are forwarded.                                                                
                                                                                   Example: ZERO_QUERY_ALLOWED_CLIENT_HEADERS=x-request-id,x-correlation-id                                                   
                                                                                                                                                                                                              
     --query-allowed-request-headers string[]                                      optional                                                                                                                   
       ZERO_QUERY_ALLOWED_REQUEST_HEADERS env                                                                                                                                                                 
                                                                                   A list of header names to forward from the incoming HTTP request to the query URL.                                         
                                                                                   Unlike allowed-client-headers (which forwards headers set by the client), these are taken                                  
                                                                                   from the HTTP request that established the connection (e.g. headers injected by a proxy or load balancer).                 
                                                                                   If a listed header is present on the request, its value is forwarded upstream under the same header name.                  
                                                                                   Header names are case-insensitive.                                                                                         
                                                                                   If not specified, no request headers are forwarded.                                                                        
                                                                                   Example: ZERO_QUERY_ALLOWED_REQUEST_HEADERS=x-forwarded-for,cf-ray                                                         
                                                                                                                                                                                                              
     --enable-crud-mutations boolean                                               default: true                                                                                                              
       ZERO_ENABLE_CRUD_MUTATIONS env                                                                                                                                                                         
                                                                                   Enables support for legacy CRUD mutations. When this is false, no connections                                              
                                                                                   are made from view-syncers to the upstream db, and push messages with CRUD mutations                                       
                                                                                   result in an InvalidPush response.                                                                                         
                                                                                                                                                                                                              
     --allow-legacy-queries boolean                                                default: false                                                                                                             
       ZERO_ALLOW_LEGACY_QUERIES env                                                                                                                                                                          
                                                                                   Allows clients to send legacy query ASTs directly to zero-cache.                                                           
                                                                                   Keep this disabled when using custom queries so that zero-cache rejects                                                    
                                                                                   client-supplied ASTs without parsing them.                                                                                 
                                                                                                                                                                                                              
     --cvr-db string                                                               optional                                                                                                                   
       ZERO_CVR_DB env                                                                                                                                                                                        
                                                                                   The Postgres database used to store CVRs. CVRs (client view records) keep track                                            
                                                                                   of the data synced to clients in order to determine the diff to send on reconnect.                                         
                                                                                   If unspecified, the upstream-db will be used.                                                                              
                                                                                                                                                                                                              
     --cvr-max-conns number                                                        default: 30                                                                                                                
       ZERO_CVR_MAX_CONNS env                                                                                                                                                                                 
                                                                                   The maximum number of connections to open to the CVR database.                                                             
                                                                                   This is divided evenly amongst sync workers.                                                                               
                                                                                                                                                                                                              
                                                                                   Note that this number must allow for at least one connection per                                                           
                                                                                   sync worker, or zero-cache will fail to start. See num-sync-workers                                                        
                                                                                                                                                                                                              
     --cvr-garbage-collection-inactivity-threshold-hours number                    default: 48                                                                                                                
       ZERO_CVR_GARBAGE_COLLECTION_INACTIVITY_THRESHOLD_HOURS env                                                                                                                                             
                                                                                   The duration after which an inactive CVR is eligible for garbage collection.                                               
                                                                                   Note that garbage collection is an incremental, periodic process which does not                                            
                                                                                   necessarily purge all eligible CVRs immediately.                                                                           
                                                                                                                                                                                                              
     --cvr-garbage-collection-initial-interval-seconds number                      default: 60                                                                                                                
       ZERO_CVR_GARBAGE_COLLECTION_INITIAL_INTERVAL_SECONDS env                                                                                                                                               
                                                                                   The initial interval at which to check and garbage collect inactive CVRs.                                                  
                                                                                   This interval is increased exponentially (up to 16 minutes) when there is                                                  
                                                                                   nothing to purge.                                                                                                          
                                                                                                                                                                                                              
     --cvr-garbage-collection-initial-batch-size number                            default: 25                                                                                                                
       ZERO_CVR_GARBAGE_COLLECTION_INITIAL_BATCH_SIZE env                                                                                                                                                     
                                                                                   The initial number of CVRs to purge per garbage collection interval.                                                       
                                                                                   This number is increased linearly if the rate of new CVRs exceeds the rate of                                              
                                                                                   purged CVRs, in order to reach a steady state.                                                                             
                                                                                                                                                                                                              
                                                                                   Setting this to 0 effectively disables CVR garbage collection.                                                             
                                                                                                                                                                                                              
     --query-hydration-stats boolean                                               optional                                                                                                                   
       ZERO_QUERY_HYDRATION_STATS env                                                                                                                                                                         
                                                                                   Track and log the number of rows considered by query hydrations which                                                      
                                                                                   take longer than log-slow-hydrate-threshold milliseconds.                                                                  
                                                                                   This is useful for debugging and performance tuning.                                                                       
                                                                                                                                                                                                              
     --enable-query-planner boolean                                                default: true                                                                                                              
       ZERO_ENABLE_QUERY_PLANNER env                                                                                                                                                                          
                                                                                   Enable the query planner for optimizing ZQL queries.                                                                       
                                                                                                                                                                                                              
                                                                                   The query planner analyzes and optimizes query execution by determining                                                    
                                                                                   the most efficient join strategies.                                                                                        
                                                                                                                                                                                                              
                                                                                   You can disable the planner if it is picking bad strategies.                                                               
                                                                                                                                                                                                              
     --enable-query-covering boolean                                               default: true                                                                                                              
       ZERO_ENABLE_QUERY_COVERING env                                                                                                                                                                         
                                                                                   Enable shadow-mode query covering detection during query hydration.                                                        
                                                                                                                                                                                                              
                                                                                   When enabled, view-syncers compare newly hydrated queries against running                                                  
                                                                                   queries with the same root table and log aggregate coverage stats.                                                         
                                                                                                                                                                                                              
                                                                                   You can disable this if covering detection adds too much CPU overhead.                                                     
                                                                                                                                                                                                              
     --yield-threshold-ms number                                                   default: 10                                                                                                                
       ZERO_YIELD_THRESHOLD_MS env                                                                                                                                                                            
                                                                                   The maximum amount of time in milliseconds that a sync worker will                                                         
                                                                                   spend in IVM (processing query hydration and advancement) before yielding                                                  
                                                                                   to the event loop. Lower values increase responsiveness and fairness at                                                    
                                                                                   the cost of reduced throughput.                                                                                            
                                                                                                                                                                                                              
     --change-db string                                                            optional                                                                                                                   
       ZERO_CHANGE_DB env                                                                                                                                                                                     
                                                                                   The Postgres database used to store recent replication log entries, in order                                               
                                                                                   to sync multiple view-syncers without requiring multiple replication slots on                                              
                                                                                   the upstream database. If unspecified, the upstream-db will be used.                                                       
                                                                                                                                                                                                              
     --change-max-conns number                                                     default: 5                                                                                                                 
       ZERO_CHANGE_MAX_CONNS env                                                                                                                                                                              
                                                                                   The maximum number of connections to open to the change database.                                                          
                                                                                   This is used by the change-streamer for catching up                                                                        
                                                                                   zero-cache replication subscriptions.                                                                                      
                                                                                                                                                                                                              
     --replica-file string                                                         default: "zero.db"                                                                                                         
       ZERO_REPLICA_FILE env                                                                                                                                                                                  
                                                                                   File path to the SQLite replica that zero-cache maintains.                                                                 
                                                                                   This can be lost, but if it is, zero-cache will have to re-replicate next                                                  
                                                                                   time it starts up.                                                                                                         
                                                                                                                                                                                                              
     --replica-vacuum-interval-hours number                                        optional                                                                                                                   
       ZERO_REPLICA_VACUUM_INTERVAL_HOURS env                                                                                                                                                                 
                                                                                   Performs a VACUUM at server startup if the specified number of hours has elapsed                                           
                                                                                   since the last VACUUM (or initial-sync). The VACUUM operation is heavyweight                                               
                                                                                   and requires double the size of the db in disk space. If unspecified, VACUUM                                               
                                                                                   operations are not performed.                                                                                              
                                                                                                                                                                                                              
     --log-level debug,info,warn,error                                             default: "info"                                                                                                            
       ZERO_LOG_LEVEL env                                                                                                                                                                                     
                                                                                                                                                                                                              
     --log-format text,json                                                        default: "text"                                                                                                            
       ZERO_LOG_FORMAT env                                                                                                                                                                                    
                                                                                   Use text for developer-friendly console logging                                                                            
                                                                                   and json for consumption by structured-logging services                                                                    
                                                                                                                                                                                                              
     --log-slow-row-threshold number                                               default: 2                                                                                                                 
       ZERO_LOG_SLOW_ROW_THRESHOLD env                                                                                                                                                                        
                                                                                   The number of ms a row must take to fetch from table-source before it is considered slow.                                  
                                                                                                                                                                                                              
     --log-slow-hydrate-threshold number                                           default: 100                                                                                                               
       ZERO_LOG_SLOW_HYDRATE_THRESHOLD env                                                                                                                                                                    
                                                                                   The number of milliseconds a query hydration must take to print a slow warning.                                            
                                                                                                                                                                                                              
     --log-ivm-sampling number                                                     default: 5000                                                                                                              
       ZERO_LOG_IVM_SAMPLING env                                                                                                                                                                              
                                                                                   How often to collect IVM metrics. 1 out of N requests will be sampled where N is this value.                               
                                                                                                                                                                                                              
     --app-id string                                                               default: "zero"                                                                                                            
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
                                                                                                                                                                                                              
     --app-publications string[]                                                   default: []                                                                                                                
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
                                                                                                                                                                                                              
     --auth-revalidate-interval-seconds number                                     default: 300                                                                                                               
       ZERO_AUTH_REVALIDATE_INTERVAL_SECONDS env                                                                                                                                                              
                                                                                   The interval in seconds between periodic /query auth revalidation for validated connections.                               
                                                                                   If unset, periodic auth revalidation is disabled.                                                                          
                                                                                                                                                                                                              
     --auth-retransform-interval-seconds number                                    default: 300                                                                                                               
       ZERO_AUTH_RETRANSFORM_INTERVAL_SECONDS env                                                                                                                                                             
                                                                                   The interval in seconds between periodic shared /query retransform work for a client group.                                
                                                                                   If unset, periodic shared retransform is disabled.                                                                         
                                                                                                                                                                                                              
     --port number                                                                 default: 4848                                                                                                              
       ZERO_PORT env                                                                                                                                                                                          
                                                                                   The port for sync connections.                                                                                             
                                                                                                                                                                                                              
     --keepalive-timeout-ms number                                                 optional                                                                                                                   
       ZERO_KEEPALIVE_TIMEOUT_MS env                                                                                                                                                                          
                                                                                   The timeout since the last /keepalive request after which the server will initiate                                         
                                                                                   a graceful shutdown. This is a workaround for AWS Elastic Container Service, which                                         
                                                                                   otherwise provides no signal that a target has been deregistered (and should thus begin                                    
                                                                                   shutdown); the cessation of health checks at /keepalive is instead used as the signal to                                   
                                                                                   drain. (ECS later sends a SIGTERM before killing the server but only allows a 30-second                                    
                                                                                   timeout before sending SIGKILL).                                                                                           
                                                                                                                                                                                                              
                                                                                   Other container runners explicitly send a SIGTERM followed by a configurable drain interval,                               
                                                                                   in which case /keepalive logic is not necessary.                                                                           
                                                                                                                                                                                                              
                                                                                   When running the server in ECS, this timeout should be set to some multiple of the health                                  
                                                                                   check interval. If the option is unset, the keepalive timeout is disabled in non-ECS environments,                         
                                                                                   and defaults to 20 seconds when run in ECS (determined by the presence of the                                              
                                                                                   ECS_CONTAINER_METADATA_URI_V4 environment variable as per                                                                  
                                                                                   https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-environment-variables.html).                               
                                                                                                                                                                                                              
     --change-streamer-uri string                                                  optional                                                                                                                   
       ZERO_CHANGE_STREAMER_URI env                                                                                                                                                                           
                                                                                   When set, connects to the change-streamer at the given URI.                                                                
                                                                                   In a multi-node setup, this should be specified in view-syncer options,                                                    
                                                                                   pointing to the replication-manager URI, which runs a change-streamer                                                      
                                                                                   on port 4849.                                                                                                              
                                                                                                                                                                                                              
     --change-streamer-mode dedicated,discover                                     default: "dedicated"                                                                                                       
       ZERO_CHANGE_STREAMER_MODE env                                                                                                                                                                          
                                                                                   As an alternative to ZERO_CHANGE_STREAMER_URI, the ZERO_CHANGE_STREAMER_MODE                                               
                                                                                   can be set to "discover" to instruct the view-syncer to connect to the                                                     
                                                                                   ip address registered by the replication-manager upon startup.                                                             
                                                                                                                                                                                                              
                                                                                   This may not work in all networking configurations, e.g. certain private                                                   
                                                                                   networking or port forwarding configurations. Using the ZERO_CHANGE_STREAMER_URI                                           
                                                                                   with an explicit routable hostname is recommended instead.                                                                 
                                                                                                                                                                                                              
                                                                                   Note: This option is ignored if the ZERO_CHANGE_STREAMER_URI is set.                                                       
                                                                                                                                                                                                              
     --change-streamer-port number                                                 optional                                                                                                                   
       ZERO_CHANGE_STREAMER_PORT env                                                                                                                                                                          
                                                                                   The port on which the change-streamer runs. This is an internal                                                            
                                                                                   protocol between the replication-manager and view-syncers, which                                                           
                                                                                   runs in the same process tree in local development or a single-node configuration.                                         
                                                                                                                                                                                                              
                                                                                   If unspecified, defaults to --port + 1.                                                                                    
                                                                                                                                                                                                              
     --change-streamer-startup-delay-ms number                                     default: 15000                                                                                                             
       ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS env                                                                                                                                                              
                                                                                   The delay to wait before the change-streamer takes over the replication stream                                             
                                                                                   (i.e. the handoff during replication-manager updates), to allow loadbalancers to register                                  
                                                                                   the task as healthy based on healthcheck parameters. Note that if a change stream request                                  
                                                                                   is received during this interval, the delay will be canceled and the takeover will happen                                  
                                                                                   immediately, since the incoming request indicates that the task is registered as a target.                                 
                                                                                                                                                                                                              
     --change-streamer-back-pressure-limit-heap-proportion number                  default: 0.04                                                                                                              
       ZERO_CHANGE_STREAMER_BACK_PRESSURE_LIMIT_HEAP_PROPORTION env                                                                                                                                           
                                                                                   The percentage of --max-old-space-size to use as a buffer for absorbing replication                                        
                                                                                   stream spikes. When the estimated amount of queued data exceeds this threshold, back pressure                              
                                                                                   is applied to the replication stream, delaying downstream sync as a result.                                                
                                                                                                                                                                                                              
                                                                                   The threshold was determined empirically with load testing. Higher thresholds have resulted                                
                                                                                   in OOMs. Note also that the byte-counting logic in the queue is strictly an underestimate of                               
                                                                                   actual memory usage (but importantly, proportionally correct), so the queue is actually                                    
                                                                                   using more than what this proportion suggests.                                                                             
                                                                                                                                                                                                              
                                                                                   This parameter is exported as an emergency knob to reduce the size of the buffer in the                                    
                                                                                   event that the server OOMs from back pressure. Resist the urge to increase this                                            
                                                                                   proportion, as it is mainly useful for absorbing periodic spikes and does not meaningfully                                 
                                                                                   affect steady-state replication throughput; the latter is determined by other factors such                                 
                                                                                   as object serialization and PG throughput                                                                                  
                                                                                                                                                                                                              
                                                                                   In other words, the back pressure limit does not constrain replication throughput;                                         
                                                                                   rather, it protects the system when the upstream throughput exceeds the downstream                                         
                                                                                   throughput.                                                                                                                
                                                                                                                                                                                                              
     --change-streamer-flow-control-consensus-timeout-proportion number            default: 2                                                                                                                 
       ZERO_CHANGE_STREAMER_FLOW_CONTROL_CONSENSUS_TIMEOUT_PROPORTION env                                                                                                                                     
                                                                                   During periodic flow control checks (every 64kb), the amount of time to wait after the majority                            
                                                                                   of subscribers have acked, proportional to that interval, after which replication will continue                            
                                                                                   even if some subscribers have yet to ack.                                                                                  
                                                                                                                                                                                                              
                                                                                   This allows a bounded amount of time for backlogged subscribers to catch up on each flush                                  
                                                                                   without forcing all subscribers to wait for the entire backlog to be processed. It is also                                 
                                                                                   useful for mitigating the effect of unresponsive subscribers due to severed websocket                                      
                                                                                   connections or pathological zombie situations (until liveness checks or laggard detection                                  
                                                                                   disconnects them).                                                                                                         
                                                                                                                                                                                                              
                                                                                   For example, if the majority of subscribers ack a message in 2.5ms, a padding proportion of                                
                                                                                   1.0 instructs replication to continue after an additional 2.5ms; for a proportion of 2.0, an                               
                                                                                   additional 5.0ms, etc. The default value of 2.0 allows for a subscriber to be 3x slower than the                           
                                                                                   majority in the steady state, while similarly bounding the extent to which a temporarily lagging                           
                                                                                   subscriber (e.g. due to catchup) slows down the fleet.                                                                     
                                                                                                                                                                                                              
                                                                                   Note that subscribers that continually exceed the timeout will eventually be disconnected and                              
                                                                                   instructed to shutdown in order to protect the healthy subscribers. It is thus important that                              
                                                                                   the proportion account for expected variance in processing speed across subscribers.                                       
                                                                                                                                                                                                              
                                                                                   Set this to a negative number to disable early flow control releases. (Not recommended, but                                
                                                                                   available as an emergency measure.)                                                                                        
                                                                                                                                                                                                              
     --change-streamer-flow-control-slow-subscriber-grace-period-seconds number    default: 30                                                                                                                
       ZERO_CHANGE_STREAMER_FLOW_CONTROL_SLOW_SUBSCRIBER_GRACE_PERIOD_SECONDS env                                                                                                                             
                                                                                   The period of time after which a lagging subscriber is disconnected and instructed to                                      
                                                                                   restart. A subscriber is considered lagging if it continuously (1) exceeds the                                             
                                                                                   consensus timeout and (2) fails to exceed the change rate of healthy subscribers. These                                    
                                                                                   conditions distinguish expected, temporary periods of slowness from pathological                                           
                                                                                   scenarios, such as zombie tasks, in which the subscriber is unlikely to recover.                                           
                                                                                                                                                                                                              
                                                                                   In particular, the second condition excludes subscribers that are performing initial                                       
                                                                                   catchup, as the rate of change for catchup must eventually exceed the rate of upstream                                     
                                                                                   changes. Note, however, that catchup rate can be legitimately slow during (rare) expensive                                 
                                                                                   operations such as index creation.                                                                                         
                                                                                                                                                                                                              
                                                                                   Thus, this grace period caps the amount of time that a lagging subscriber degrades overall                                 
                                                                                   throughput, but should be long enough to allow for legitimate intervals of slowness.                                       
                                                                                                                                                                                                              
                                                                                   Note that in the pathological case where a catchup operation exceeds this grace period,                                    
                                                                                   the system will eventually recover after the next backup/restore cycle, as the expensive                                   
                                                                                   operation (e.g. index creation) will have been applied to the restored replica and no longer                               
                                                                                   executed during catchup.                                                                                                   
                                                                                                                                                                                                              
                                                                                   Set this to 0 or a negative number to disable laggard detection. (Not recommended, but                                     
                                                                                   available as an emergency measure.)                                                                                        
                                                                                                                                                                                                              
     --task-id string                                                              optional                                                                                                                   
       ZERO_TASK_ID env                                                                                                                                                                                       
                                                                                   Globally unique identifier for the zero-cache instance.                                                                    
                                                                                                                                                                                                              
                                                                                   Setting this to a platform specific task identifier can be useful for debugging.                                           
                                                                                   If unspecified, zero-cache will attempt to extract the TaskARN if run from within                                          
                                                                                   an AWS ECS container, and otherwise use a random string.                                                                   
                                                                                                                                                                                                              
     --per-user-mutation-limit-max number                                          optional                                                                                                                   
       ZERO_PER_USER_MUTATION_LIMIT_MAX env                                                                                                                                                                   
                                                                                   The maximum mutations per user within the specified windowMs.                                                              
                                                                                   If unset, no rate limiting is enforced.                                                                                    
                                                                                                                                                                                                              
     --per-user-mutation-limit-window-ms number                                    default: 60000                                                                                                             
       ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS env                                                                                                                                                             
                                                                                   The sliding window over which the perUserMutationLimitMax is enforced.                                                     
                                                                                                                                                                                                              
     --num-sync-workers number                                                     optional                                                                                                                   
       ZERO_NUM_SYNC_WORKERS env                                                                                                                                                                              
                                                                                   The number of processes to use for view syncing.                                                                           
                                                                                   Leave this unset to use the maximum available parallelism.                                                                 
                                                                                   If set to 0, the server runs without sync workers, which is the                                                            
                                                                                   configuration for running the replication-manager.                                                                         
                                                                                                                                                                                                              
     --auto-reset boolean                                                          default: true                                                                                                              
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
                                                                                                                                                                                                              
     --replication-lag-report-interval-ms number                                   default: 30000                                                                                                             
       ZERO_REPLICATION_LAG_REPORT_INTERVAL_MS env                                                                                                                                                            
                                                                                   The minimum interval at which replication lag reports are written upstream and                                             
                                                                                   reported via the zero.replication.total_lag opentelemetry metric. If                                                       
                                                                                   an expected report is not received before the next interval, Zero retries with                                             
                                                                                   a new report and increments zero.replication.lag_report_retries. A                                                         
                                                                                   negative or 0 value disables lag reporting.                                                                                
                                                                                                                                                                                                              
                                                                                   This monitoring feature is only support on the postgres upstream type.                                                     
                                                                                                                                                                                                              
     --admin-password string                                                       optional                                                                                                                   
       ZERO_ADMIN_PASSWORD env                                                                                                                                                                                
                                                                                   A password used to administer zero-cache server, for example to access the                                                 
                                                                                   /statz endpoint.                                                                                                           
                                                                                                                                                                                                              
                                                                                   A password is optional in development mode but required in production mode.                                                
                                                                                                                                                                                                              
     --websocket-compression boolean                                               default: false                                                                                                             
       ZERO_WEBSOCKET_COMPRESSION env                                                                                                                                                                         
                                                                                   Enable WebSocket per-message deflate compression.                                                                          
                                                                                                                                                                                                              
                                                                                   Compression can reduce bandwidth usage for sync traffic but                                                                
                                                                                   increases CPU usage on both client and server. Disabled by default.                                                        
                                                                                                                                                                                                              
                                                                                   See: https://github.com/websockets/ws#websocket-compression                                                                
                                                                                                                                                                                                              
     --websocket-compression-options string                                        optional                                                                                                                   
       ZERO_WEBSOCKET_COMPRESSION_OPTIONS env                                                                                                                                                                 
                                                                                   JSON string containing WebSocket compression options.                                                                      
                                                                                                                                                                                                              
                                                                                   Only used if websocketCompression is enabled.                                                                              
                                                                                                                                                                                                              
                                                                                   Example: {"zlibDeflateOptions":{"level":3},"threshold":1024}                                                               
                                                                                                                                                                                                              
                                                                                   See https://github.com/websockets/ws/blob/master/doc/ws.md#new-websocketserveroptions-callback for available options.      
                                                                                                                                                                                                              
     --websocket-max-payload-bytes number                                          default: 10485760                                                                                                          
       ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES env                                                                                                                                                                   
                                                                                   Maximum size of incoming WebSocket messages in bytes.                                                                      
                                                                                                                                                                                                              
                                                                                   Messages exceeding this limit are rejected before parsing.                                                                 
                                                                                   Default: 10MB (10 * 1024 * 1024 = 10485760)                                                                                
                                                                                                                                                                                                              
     --litestream-executable string                                                optional                                                                                                                   
       ZERO_LITESTREAM_EXECUTABLE env                                                                                                                                                                         
                                                                                   Path to the litestream executable. This must be built from the                                                             
                                                                                   rocicorp/litestream fork. Support for the official binary at v0.5.x                                                        
                                                                                   is planned.                                                                                                                
                                                                                                                                                                                                              
     --litestream-executable-v5 string                                             optional                                                                                                                   
       ZERO_LITESTREAM_EXECUTABLE_V5 env                                                                                                                                                                      
                                                                                   The v0.5.x litestream executable which is used for restoring the backup                                                    
                                                                                   backup when ZERO_LITESTREAM_RESTORE_USING_V5 is specified.                                                                 
                                                                                   litestream v0.5.8+ can restore from both v0.3.x and v0.5.x backup formats,                                                 
                                                                                   affording forwards compatibility with a future zero-cache                                                                  
                                                                                   version that will use litestream v0.5.x to backup the replica.                                                             
                                                                                                                                                                                                              
     --litestream-restore-using-v5 boolean                                         default: true                                                                                                              
       ZERO_LITESTREAM_RESTORE_USING_V5 env                                                                                                                                                                   
                                                                                   Restores the backup using the ZERO_LITESTREAM_EXECUTABLE_V5 if specified.                                                  
                                                                                   This provides a recovery path if rolling back from ZERO_LITESTREAM_BACKUP_USING_V5                                         
                                                                                   as v5 restores from both v3 and v5 backups (whichever is more recent).                                                     
                                                                                                                                                                                                              
     --litestream-backup-using-v5 boolean                                          default: false                                                                                                             
       ZERO_LITESTREAM_BACKUP_USING_V5 env                                                                                                                                                                    
                                                                                   Backs up the replica using Litestream v0.5.x and monitors cleanup                                                          
                                                                                   watermarks by reading the backup through the Litestream SQLite VFS.                                                        
                                                                                   This requires ZERO_LITESTREAM_RESTORE_USING_V5 and                                                                         
                                                                                   ZERO_LITESTREAM_VFS_QUERY_EXECUTABLE                                                                                       
                                                                                                                                                                                                              
     --litestream-config-path string                                               default: "./src/services/litestream/config.yml"                                                                            
       ZERO_LITESTREAM_CONFIG_PATH env                                                                                                                                                                        
                                                                                   Path to the litestream yaml config file. zero-cache will run this with its                                                 
                                                                                   environment variables, which can be referenced in the file via \${ENV}                                                      
                                                                                   substitution, for example:                                                                                                 
                                                                                   * ZERO_REPLICA_FILE for the db path                                                                                        
                                                                                   * ZERO_LITESTREAM_BACKUP_LOCATION for the db replica url                                                                   
                                                                                   * ZERO_LITESTREAM_LOG_LEVEL for the log level                                                                              
                                                                                   * ZERO_LOG_FORMAT for the log type                                                                                         
                                                                                                                                                                                                              
     --litestream-config-path-v5 string                                            default: "./src/services/litestream/config-v5.yml"                                                                         
       ZERO_LITESTREAM_CONFIG_PATH_V5 env                                                                                                                                                                     
                                                                                   Path to the litestream v5 yaml config file. zero-cache will run this with its                                              
                                                                                   environment variables, which can be referenced in the file via \${ENV}                                                      
                                                                                   substitution, for example:                                                                                                 
                                                                                   * ZERO_REPLICA_FILE for the db path                                                                                        
                                                                                   * ZERO_LITESTREAM_BACKUP_LOCATION for the db replica url                                                                   
                                                                                   * ZERO_LITESTREAM_LOG_LEVEL for the log level                                                                              
                                                                                   * ZERO_LOG_FORMAT for the log type                                                                                         
                                                                                                                                                                                                              
     --litestream-vfs-query-executable string                                      optional                                                                                                                   
       ZERO_LITESTREAM_VFS_QUERY_EXECUTABLE env                                                                                                                                                               
                                                                                   Path to the rocicorp vfs-query executable that runs the VFS-based                                                          
                                                                                   polling of backup watermark. This is required when backing up with V5.                                                     
                                                                                                                                                                                                              
     --litestream-vfs-poll-interval-ms number                                      default: 15000                                                                                                             
       ZERO_LITESTREAM_VFS_POLL_INTERVAL_MS env                                                                                                                                                               
                                                                                   Interval in milliseconds litestream vfs extension polls the backup store (e.g. s3)                                         
                                                                                   to determine the most recent backup.                                                                                       
                                                                                                                                                                                                              
                                                                                   This, in turn, influences how quickly new backups are confirmed, allowing the                                              
                                                                                   change-streamer to ack the upstream change-source (e.g. replication slot).                                                 
                                                                                                                                                                                                              
     --litestream-vfs-poll-timeout-ms number                                       default: 10000                                                                                                             
       ZERO_LITESTREAM_VFS_POLL_TIMEOUT_MS env                                                                                                                                                                
                                                                                   Timeout in milliseconds for requests to the Litestream VFS poller.                                                         
                                                                                                                                                                                                              
     --litestream-vfs-log-file string                                              optional                                                                                                                   
       ZERO_LITESTREAM_VFS_LOG_FILE env                                                                                                                                                                       
                                                                                   Optional file path for logs emitted by the Litestream VFS native                                                           
                                                                                   extension. If unset, the extension writes to stdout.                                                                       
                                                                                                                                                                                                              
     --litestream-log-level debug,info,warn,error                                  default: "warn"                                                                                                            
       ZERO_LITESTREAM_LOG_LEVEL env                                                                                                                                                                          
                                                                                                                                                                                                              
     --litestream-backup-url string                                                optional                                                                                                                   
       ZERO_LITESTREAM_BACKUP_URL env                                                                                                                                                                         
                                                                                   The location of the litestream backup, usually an s3:// URL.                                                               
                                                                                   This is only consulted by the replication-manager.                                                                         
                                                                                   view-syncers receive this information from the replication-manager.                                                        
                                                                                                                                                                                                              
     --litestream-endpoint string                                                  optional                                                                                                                   
       ZERO_LITESTREAM_ENDPOINT env                                                                                                                                                                           
                                                                                   The S3-compatible endpoint URL to use for the litestream backup. Only required for non-AWS services.                       
                                                                                   The replication-manager and view-syncers must have the same endpoint.                                                      
                                                                                                                                                                                                              
     --litestream-region string                                                    optional                                                                                                                   
       ZERO_LITESTREAM_REGION env                                                                                                                                                                             
                                                                                   The AWS region for the litestream backup bucket. Required for non-standard AWS partitions                                  
                                                                                   (e.g. GovCloud us-gov-west-1) where Litestream cannot auto-detect the region.                                              
                                                                                   The replication-manager and view-syncers must have the same region.                                                        
                                                                                                                                                                                                              
     --litestream-port number                                                      optional                                                                                                                   
       ZERO_LITESTREAM_PORT env                                                                                                                                                                               
                                                                                   Port on which litestream exports metrics, used to determine the replication                                                
                                                                                   watermark up to which it is safe to purge change log records.                                                              
                                                                                                                                                                                                              
                                                                                   If unspecified, defaults to --port + 2.                                                                                    
                                                                                                                                                                                                              
     --litestream-checkpoint-threshold-mb number                                   default: 40                                                                                                                
       ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB env                                                                                                                                                            
                                                                                   The size of the WAL file at which to perform an SQlite checkpoint to apply                                                 
                                                                                   the writes in the WAL to the main database file. Each checkpoint creates                                                   
                                                                                   a new WAL segment file that will be backed up by litestream (v3). Smaller thresholds                                       
                                                                                   may improve read performance, at the expense of creating more files to download                                            
                                                                                   when restoring the replica from the backup.                                                                                
                                                                                                                                                                                                              
                                                                                   This setting is only relevant when replicating with litestream v3, and is ignored                                          
                                                                                   when replicating with litestream v5.                                                                                       
                                                                                                                                                                                                              
     --litestream-min-checkpoint-page-count number                                 optional                                                                                                                   
       ZERO_LITESTREAM_MIN_CHECKPOINT_PAGE_COUNT env                                                                                                                                                          
                                                                                   The WAL page count at which SQLite attempts a PASSIVE checkpoint, which                                                    
                                                                                   transfers pages to the main database file without blocking writers.                                                        
                                                                                   Defaults to checkpointThresholdMB * 250 (since SQLite page size is 4KB).                                                   
                                                                                                                                                                                                              
                                                                                   This setting is only relevant when replicating with litestream v3, and is ignored                                          
                                                                                   when replicating with litestream v5.                                                                                       
                                                                                                                                                                                                              
     --litestream-max-checkpoint-page-count number                                 optional                                                                                                                   
       ZERO_LITESTREAM_MAX_CHECKPOINT_PAGE_COUNT env                                                                                                                                                          
                                                                                   The WAL page count at which SQLite performs a RESTART checkpoint, which                                                    
                                                                                   blocks writers until complete. Defaults to minCheckpointPageCount * 10.                                                    
                                                                                   Set to 0 to disable RESTART checkpoints entirely.                                                                          
                                                                                                                                                                                                              
                                                                                   This setting is only relevant when replicating with litestream v3, and is ignored                                          
                                                                                   when replicating with litestream v5.                                                                                       
                                                                                                                                                                                                              
     --litestream-incremental-backup-interval-minutes number                       default: 5                                                                                                                 
       ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES env                                                                                                                                                
                                                                                   The interval between incremental v3 backups of the replica. Shorter intervals                                              
                                                                                   reduce the amount of change history that needs to be replayed when catching                                                
                                                                                   up a new view-syncer, at the expense of increasing the number of files needed                                              
                                                                                   to download for the initial litestream restore.                                                                            
                                                                                                                                                                                                              
                                                                                   This option only applies to litestream v3 backups and will be deprecated/removed                                           
                                                                                   once the zero-cache is transitioned to litestream v5. For configuring v5 backup                                            
                                                                                   frequency, use ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_SECONDS.                                                        
                                                                                                                                                                                                              
     --litestream-incremental-backup-interval-seconds number                       default: 15                                                                                                                
       ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_SECONDS env                                                                                                                                                
                                                                                   The interval between incremental v5 backups of the replica. With litestream v5                                             
                                                                                   the upstream change source is not ACKed until the corresponding changes have been                                          
                                                                                   applied to the replica and backed up by litestream. As such, shorter intervals                                             
                                                                                   incur a higher number of backup storage writes and files managed (e.g. in s3),                                             
                                                                                   while longer intervals result requiring a larger buffer for changes upstream                                               
                                                                                   (e.g. per-replication slot wal records). The default value of 15 seconds targets                                           
                                                                                   an s3 API cost of ~$1/month (not counting storage costs).                                                                  
                                                                                                                                                                                                              
                                                                                   This option only applies to litestream v5 backups. For v3 backups, use                                                     
                                                                                   ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES.                                                                       
                                                                                                                                                                                                              
     --litestream-snapshot-backup-interval-hours number                            default: 4                                                                                                                 
       ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_HOURS env                                                                                                                                                     
                                                                                   The interval between snapshot backups of the replica. Snapshot backups                                                     
                                                                                   make a full copy of the database to a new litestream generation. This                                                      
                                                                                   improves restore time at the expense of bandwidth. Applications with a                                                     
                                                                                   large database and low write rate can increase this interval to reduce                                                     
                                                                                   network usage for backups (litestream defaults to 24 hours).                                                               
                                                                                                                                                                                                              
                                                                                   This setting is applied when replicating with either litestream v3 or v5.                                                  
                                                                                   Note, however, that snapshots are generally not needed to improve restore time                                             
                                                                                   with v5, and so a longer interval (e.g. the litestream default of 24h) is fine.                                            
                                                                                                                                                                                                              
     --litestream-restore-parallelism number                                       default: 48                                                                                                                
       ZERO_LITESTREAM_RESTORE_PARALLELISM env                                                                                                                                                                
                                                                                   The number of WAL files to download in parallel when performing the                                                        
                                                                                   initial restore of the replica from the backup.                                                                            
                                                                                                                                                                                                              
     --litestream-multipart-concurrency number                                     default: 48                                                                                                                
       ZERO_LITESTREAM_MULTIPART_CONCURRENCY env                                                                                                                                                              
                                                                                   The number of parts (of size --litestream-multipart-size bytes)                                                            
                                                                                   to upload or download in parallel when backing up or restoring the snapshot.                                               
                                                                                                                                                                                                              
     --litestream-multipart-size number                                            default: 16777216                                                                                                          
       ZERO_LITESTREAM_MULTIPART_SIZE env                                                                                                                                                                     
                                                                                   The size of each part when uploading or downloading the snapshot with                                                      
                                                                                   --multipart-concurrency. Note that up to concurrency * size                                                                
                                                                                   bytes of memory are used when backing up or restoring the snapshot.                                                        
                                                                                                                                                                                                              
     --storage-db-tmp-dir string                                                   optional                                                                                                                   
       ZERO_STORAGE_DB_TMP_DIR env                                                                                                                                                                            
                                                                                   tmp directory for IVM operator storage. Leave unset to use os.tmpdir()                                                     
                                                                                                                                                                                                              
     --initial-sync-table-copy-workers number                                      default: 5                                                                                                                 
       ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS env                                                                                                                                                               
                                                                                   The number of parallel workers used to copy tables during initial sync.                                                    
                                                                                   Each worker uses a database connection and will buffer up to (approximately)                                               
                                                                                   10 MB of table data in memory during initial sync. Increasing the number of                                                
                                                                                   workers may improve initial sync speed; however, note that local disk throughput                                           
                                                                                   (i.e. IOPS), upstream CPU, and network bandwidth may also be bottlenecks.                                                  
                                                                                                                                                                                                              
     --initial-sync-text-copy boolean                                              default: false                                                                                                             
       ZERO_INITIAL_SYNC_TEXT_COPY env                                                                                                                                                                        
                                                                                   Use text-format COPY instead of binary COPY for initial sync and                                                           
                                                                                   backfill streaming. This is slower but can work around issues with                                                         
                                                                                   binary encoding of certain data types.                                                                                     
                                                                                                                                                                                                              
     --shadow-sync-enabled boolean                                                 default: false                                                                                                             
       ZERO_SHADOW_SYNC_ENABLED env                                                                                                                                                                           
                                                                                   Periodically exercises the initial-sync code path against a sample of                                                      
                                                                                   rows from every published table, writing to a throwaway SQLite database.                                                   
                                                                                   This acts as a canary: if the real initial-sync path ever breaks (schema                                                   
                                                                                   drift, PG version quirks, etc.), the shadow run fails before a customer                                                    
                                                                                   actually needs a full reset.                                                                                               
                                                                                                                                                                                                              
     --shadow-sync-interval-hours number                                           default: 12                                                                                                                
       ZERO_SHADOW_SYNC_INTERVAL_HOURS env                                                                                                                                                                    
                                                                                   The interval between shadow initial-sync runs, in hours. The first                                                         
                                                                                   run fires within [2/3, 1) of this interval after startup, so the                                                           
                                                                                   canary completes at least once per task lifetime (the replication                                                          
                                                                                   manager is restarted every ~24h) while still jittering so a fleet                                                          
                                                                                   restart does not cause all tasks to canary simultaneously.                                                                 
                                                                                                                                                                                                              
     --shadow-sync-sample-rate number                                              default: 0.1                                                                                                               
       ZERO_SHADOW_SYNC_SAMPLE_RATE env                                                                                                                                                                       
                                                                                   The BERNOULLI sampling rate for each table (0 < rate <= 1). A value of                                                     
                                                                                   1 disables sampling and copies all rows (still subject to                                                                  
                                                                                   max-rows-per-table).                                                                                                       
                                                                                                                                                                                                              
     --shadow-sync-max-rows-per-table number                                       default: 10000                                                                                                             
       ZERO_SHADOW_SYNC_MAX_ROWS_PER_TABLE env                                                                                                                                                                
                                                                                   The hard upper bound on rows copied per table per shadow run. Guards                                                       
                                                                                   against unexpectedly large tables consuming disk / upstream bandwidth.                                                     
                                                                                                                                                                                                              
     --lazy-startup boolean                                                        default: false                                                                                                             
       ZERO_LAZY_STARTUP env                                                                                                                                                                                  
                                                                                   Delay starting the majority of zero-cache until first request.                                                             
                                                                                                                                                                                                              
                                                                                   This is mainly intended to avoid connecting to Postgres replication stream                                                 
                                                                                   until the first request is received, which can be useful i.e., for preview instances.                                      
                                                                                                                                                                                                              
                                                                                   Currently only supported in single-node mode.                                                                              
                                                                                                                                                                                                              
     --server-version string                                                       optional                                                                                                                   
       ZERO_SERVER_VERSION env                                                                                                                                                                                
                                                                                   The version string outputted to logs when the server starts up.                                                            
                                                                                                                                                                                                              
     --enable-telemetry boolean                                                    default: true                                                                                                              
       ZERO_ENABLE_TELEMETRY env                                                                                                                                                                              
                                                                                   Set to false to opt out of telemetry collection.                                                                           
                                                                                                                                                                                                              
                                                                                   This helps us improve Zero by collecting anonymous usage data.                                                             
                                                                                   Setting the DO_NOT_TRACK environment variable also disables telemetry.                                                     
                                                                                                                                                                                                              
     --cloud-event-sink-env string                                                 optional                                                                                                                   
       ZERO_CLOUD_EVENT_SINK_ENV env                                                                                                                                                                          
                                                                                   ENV variable containing a URI to a CloudEvents sink. When set, ZeroEvents                                                  
                                                                                   will be published to the sink as the data field of CloudEvents.                                                            
                                                                                   The source field of the CloudEvents will be set to the ZERO_TASK_ID,                                                       
                                                                                   along with any extension attributes specified by the ZERO_CLOUD_EVENT_EXTENSION_OVERRIDES_ENV.                             
                                                                                                                                                                                                              
                                                                                   This configuration is modeled to easily integrate with a knative K_SINK binding,                                           
                                                                                   (i.e. https://github.com/knative/eventing/blob/main/docs/spec/sources.md#sinkbinding).                                     
                                                                                   However, any CloudEvents sink can be used.                                                                                 
                                                                                                                                                                                                              
     --cloud-event-extension-overrides-env string                                  optional                                                                                                                   
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

test('--enable-query-covering can be disabled', () => {
  const {config} = parseOptionsAdvanced(zeroOptions, {
    argv: ['--enable-query-covering', 'false'],
    envNamePrefix: 'ZERO_',
    allowUnknown: false,
    allowPartial: true,
  });

  expect(config.enableQueryCovering).toBe(false);
});

test('legacy queries are disabled by default', () => {
  const {config} = parseOptionsAdvanced(zeroOptions, {
    envNamePrefix: 'ZERO_',
    allowUnknown: false,
    allowPartial: true,
  });

  expect(config.allowLegacyQueries).toBe(false);
});

test('ZERO_ALLOW_LEGACY_QUERIES enables legacy queries', () => {
  const {config} = parseOptionsAdvanced(zeroOptions, {
    envNamePrefix: 'ZERO_',
    allowUnknown: false,
    allowPartial: true,
    env: {ZERO_ALLOW_LEGACY_QUERIES: 'true'},
  });

  expect(config.allowLegacyQueries).toBe(true);
});

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
