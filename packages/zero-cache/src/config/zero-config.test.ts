import {Ansis} from 'ansis';
import {expect, test, vi} from 'vitest';
import {ExitAfterUsage, parseOptions} from './config.js';
import {zeroOptions} from './zero-config.js';

// TODO: Switch (back) to strip-ansi
const ansis = new Ansis();

// Tip: Rerun tests with -u to update the snapshot.
test('zero-cache --help', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptions(zeroOptions, ['--help'], 'ZERO_', {}, logger),
  ).toThrow(ExitAfterUsage);
  expect(logger.info).toHaveBeenCalledOnce();
  expect(ansis.strip(logger.info.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --upstreamDB string                           required                                                                             
       ZERO_UPSTREAM_DB env                                                                                                             
                                                   The "upstream" authoritative postgres database.                                      
                                                   In the future we will support other types of upstream besides PG.                    
                                                                                                                                        
     --cvrDB string                                required                                                                             
       ZERO_CVR_DB env                                                                                                                  
                                                   A separate Postgres database we use to store CVRs. CVRs (client view records)        
                                                   keep track of which clients have which data. This is how we know what diff to        
                                                   send on reconnect. It can be same database as above, but it makes most sense         
                                                   for it to be a separate "database" in the same postgres "cluster".                   
                                                                                                                                        
     --changeDB string                             required                                                                             
       ZERO_CHANGE_DB env                                                                                                               
                                                   Yet another Postgres database, used to store a replication log.                      
                                                                                                                                        
     --replicaFile string                          required                                                                             
       ZERO_REPLICA_FILE env                                                                                                            
                                                   File path to the SQLite replica that zero-cache maintains.                           
                                                   This can be lost, but if it is, zero-cache will have to re-replicate next            
                                                   time it starts up.                                                                   
                                                                                                                                        
     --logLevel debug,info,warn,error              default: "info"                                                                      
       ZERO_LOG_LEVEL env                                                                                                               
                                                                                                                                        
     --logFormat text,json                         default: "text"                                                                      
       ZERO_LOG_FORMAT env                                                                                                              
                                                   Use text for developer-friendly console logging                                      
                                                   and json for consumption by structured-logging services                              
                                                                                                                                        
     --shardID string                              default: "0"                                                                         
       ZERO_SHARD_ID env                                                                                                                
                                                   Unique identifier for the zero-cache shard.                                          
                                                                                                                                        
                                                   A shard presents a logical partition of the upstream database, delineated            
                                                   by a set of publications and managed by a dedicated replication slot.                
                                                                                                                                        
                                                   A shard's zero clients table and shard-internal functions are stored in              
                                                   the zero_{id} schema in the upstream database.                                       
                                                                                                                                        
     --shardPublications string[]                  default: []                                                                          
       ZERO_SHARD_PUBLICATIONS env                                                                                                      
                                                   Postgres PUBLICATIONs that define the partition of the upstream                      
                                                   replicated to the shard. All publication names must begin with the prefix            
                                                   zero_, and all tables must be in the public schema.                                  
                                                                                                                                        
                                                   If unspecified, zero-cache will create and use a zero_public publication that        
                                                   publishes all tables in the public schema, i.e.:                                     
                                                                                                                                        
                                                   CREATE PUBLICATION zero_public FOR TABLES IN SCHEMA public;                          
                                                                                                                                        
                                                   Note that once a shard has begun syncing data, this list of publications             
                                                   cannot be changed, and zero-cache will refuse to start if a specified                
                                                   value differs from what was originally synced.                                       
                                                                                                                                        
                                                   To use a different set of publications, a new shard should be created.               
                                                                                                                                        
     --jwtSecret string                            optional                                                                             
       ZERO_JWT_SECRET env                                                                                                              
                                                   JWT secret for verifying authentication tokens.                                      
                                                                                                                                        
     --perUserMutationLimitMax number              optional                                                                             
       ZERO_PER_USER_MUTATION_LIMIT_MAX env                                                                                             
                                                   The maximum mutations per user within the specified windowMs.                        
                                                   If unset, no rate limiting is enforced.                                              
                                                                                                                                        
     --perUserMutationLimitWindowMs number         default: 60000                                                                       
       ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS env                                                                                       
                                                   The sliding window over which the perUserMutationLimitMax is enforced.               
                                                                                                                                        
     --numSyncWorkers number                       optional                                                                             
       ZERO_NUM_SYNC_WORKERS env                                                                                                        
                                                   The number of processes to use for view syncing.                                     
                                                   Leave this unset to use the maximum available parallelism.                           
                                                   If set to 0, the server runs without sync workers, which is the                      
                                                   configuration for running the replication-manager.                                   
                                                                                                                                        
     --changeStreamerURI string                    optional                                                                             
       ZERO_CHANGE_STREAMER_URI env                                                                                                     
                                                   When unset, the zero-cache runs its own replication-manager                          
                                                   (i.e. change-streamer). In production, this should be set to                         
                                                   the replication-manager URI, which runs a change-streamer                            
                                                   on port 4849.                                                                        
                                                                                                                                        
     --litestream boolean                          optional                                                                             
       ZERO_LITESTREAM env                                                                                                              
                                                   Indicates that a litestream replicate process is backing up the                      
                                                   replicaDBFile. This should be the production configuration for the                   
                                                   replication-manager. It is okay to run this in development too.                      
                                                                                                                                        
                                                   Note that this flag does actually run litestream; rather, it                         
                                                   configures the internal replication logic to operate on the DB file in               
                                                   a manner that is compatible with litestream.                                         
                                                                                                                                        
     --storageDBTmpDir string                      optional                                                                             
       ZERO_STORAGE_DB_TMP_DIR env                                                                                                      
                                                   tmp directory for IVM operator storage. Leave unset to use os.tmpdir()               
                                                                                                                                        
     --warmWebsocket number                        optional                                                                             
       ZERO_WARM_WEBSOCKET env                                                                                                          
                                                   For internal experimentation. Do not use this flag, as it will go away.              
                                                                                                                                        
    "
  `);
});
