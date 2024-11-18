import stripAnsi from 'strip-ansi';
import {expect, test, vi} from 'vitest';
import {ExitAfterUsage, parseOptions} from './config.js';
import {zeroOptions} from './zero-config.js';

// Tip: Rerun tests with -u to update the snapshot.
test('zero-cache --help', () => {
  const logger = {info: vi.fn()};
  expect(() =>
    parseOptions(zeroOptions, ['--help'], 'ZERO_', {}, logger),
  ).toThrow(ExitAfterUsage);
  expect(logger.info).toHaveBeenCalledOnce();
  expect(stripAnsi(logger.info.mock.calls[0][0])).toMatchInlineSnapshot(`
    "
     --upstream-db string                          required                                                                      
       ZERO_UPSTREAM_DB env                                                                                                      
                                                   The "upstream" authoritative postgres database.                               
                                                   In the future we will support other types of upstream besides PG.             
                                                                                                                                 
     --upstream-max-conns number                   default: 20                                                                   
       ZERO_UPSTREAM_MAX_CONNS env                                                                                               
                                                   The maximum number of connections to open to the upstream database            
                                                   for committing mutations. This is divided evenly amongst sync workers.        
                                                   In addition to this number, zero-cache uses one connection for the            
                                                   replication stream.                                                           
                                                                                                                                 
                                                   Note that this number must allow for at least one connection per              
                                                   sync worker, or zero-cache will fail to start. See --numSyncWorkers           
                                                                                                                                 
     --cvr-db string                               required                                                                      
       ZERO_CVR_DB env                                                                                                           
                                                   A separate Postgres database we use to store CVRs. CVRs (client view records) 
                                                   keep track of which clients have which data. This is how we know what diff to 
                                                   send on reconnect. It can be same database as above, but it makes most sense  
                                                   for it to be a separate "database" in the same postgres "cluster".            
                                                                                                                                 
     --cvr-max-conns number                        default: 30                                                                   
       ZERO_CVR_MAX_CONNS env                                                                                                    
                                                   The maximum number of connections to open to the CVR database.                
                                                   This is divided evenly amongst sync workers.                                  
                                                                                                                                 
                                                   Note that this number must allow for at least one connection per              
                                                   sync worker, or zero-cache will fail to start. See --numSyncWorkers           
                                                                                                                                 
     --change-db string                            required                                                                      
       ZERO_CHANGE_DB env                                                                                                        
                                                   Yet another Postgres database, used to store a replication log.               
                                                                                                                                 
     --change-max-conns number                     default: 1                                                                    
       ZERO_CHANGE_MAX_CONNS env                                                                                                 
                                                   The maximum number of connections to open to the change database.             
                                                   This is used by the change-streamer for catching up                           
                                                   zero-cache replication subscriptions.                                         
                                                                                                                                 
     --replica-file string                         required                                                                      
       ZERO_REPLICA_FILE env                                                                                                     
                                                   File path to the SQLite replica that zero-cache maintains.                    
                                                   This can be lost, but if it is, zero-cache will have to re-replicate next     
                                                   time it starts up.                                                            
                                                                                                                                 
     --log-level debug,info,warn,error             default: "info"                                                               
       ZERO_LOG_LEVEL env                                                                                                        
                                                                                                                                 
     --log-format text,json                        default: "text"                                                               
       ZERO_LOG_FORMAT env                                                                                                       
                                                   Use text for developer-friendly console logging                               
                                                   and json for consumption by structured-logging services                       
                                                                                                                                 
     --shard-id string                             default: "0"                                                                  
       ZERO_SHARD_ID env                                                                                                         
                                                   Unique identifier for the zero-cache shard.                                   
                                                                                                                                 
                                                   A shard presents a logical partition of the upstream database, delineated     
                                                   by a set of publications and managed by a dedicated replication slot.         
                                                                                                                                 
                                                   A shard's zero clients table and shard-internal functions are stored in       
                                                   the zero_{id} schema in the upstream database.                                
                                                                                                                                 
                                                   Due to constraints on replication slot names, a shard ID may only consist of  
                                                   lower-case letters, numbers, and the underscore character.                    
                                                                                                                                 
     --shard-publications string[]                 default: []                                                                   
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
                                                                                                                                 
     --port number                                 default: 4848                                                                 
       ZERO_PORT env                                                                                                             
                                                   The main port for client connections.                                         
                                                   Internally, zero-cache will also listen on the 2 ports after --port.          
                                                                                                                                 
     --change-streamer-port number                 optional                                                                      
       ZERO_CHANGE_STREAMER_PORT env                                                                                             
                                                   The port on which the change-streamer runs. This is an internal               
                                                   protocol between the replication-manager and zero-cache, which                
                                                   runs in the same process in local development.                                
                                                                                                                                 
                                                   If unspecified, defaults to --port + 1.                                       
                                                                                                                                 
     --heartbeat-monitor-port number               optional                                                                      
       ZERO_HEARTBEAT_MONITOR_PORT env                                                                                           
                                                   The port on which the heartbeat monitor listens for heartbeat                 
                                                   health checks. Once health checks are received at this port,                  
                                                   the monitor considers it a keepalive signal and triggers a drain              
                                                   if health checks stop for more than 15 seconds. If health checks              
                                                   never arrive on this port, the monitor does nothing (i.e. opt-in).            
                                                                                                                                 
                                                   If unspecified, defaults to --port + 2.                                       
                                                                                                                                 
     --jwt-secret string                           optional                                                                      
       ZERO_JWT_SECRET env                                                                                                       
                                                   JWT secret for verifying authentication tokens.                               
                                                                                                                                 
     --per-user-mutation-limit-max number          optional                                                                      
       ZERO_PER_USER_MUTATION_LIMIT_MAX env                                                                                      
                                                   The maximum mutations per user within the specified windowMs.                 
                                                   If unset, no rate limiting is enforced.                                       
                                                                                                                                 
     --per-user-mutation-limit-window-ms number    default: 60000                                                                
       ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS env                                                                                
                                                   The sliding window over which the perUserMutationLimitMax is enforced.        
                                                                                                                                 
     --num-sync-workers number                     optional                                                                      
       ZERO_NUM_SYNC_WORKERS env                                                                                                 
                                                   The number of processes to use for view syncing.                              
                                                   Leave this unset to use the maximum available parallelism.                    
                                                   If set to 0, the server runs without sync workers, which is the               
                                                   configuration for running the replication-manager.                            
                                                                                                                                 
     --change-streamer-uri string                  optional                                                                      
       ZERO_CHANGE_STREAMER_URI env                                                                                              
                                                   When unset, the zero-cache runs its own replication-manager                   
                                                   (i.e. change-streamer). In production, this should be set to                  
                                                   the replication-manager URI, which runs a change-streamer                     
                                                   on port 4849.                                                                 
                                                                                                                                 
     --auto-reset boolean                          optional                                                                      
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
                                                                                                                                 
                                                   Moreover, auto-reset is only supported for single-node configurations         
                                                   with a permanent volume for the replica. Specifically, it is incompatible     
                                                   with the litestream option, and will be ignored with a warning if             
                                                   set in combination with litestream.                                           
                                                                                                                                 
     --litestream boolean                          optional                                                                      
       ZERO_LITESTREAM env                                                                                                       
                                                   Indicates that a litestream replicate process is backing up the               
                                                   replica-file. This should be the production configuration for the             
                                                   replication-manager. It is okay to run this in development too.               
                                                                                                                                 
                                                   Note that this flag does not actually run litestream; rather, it              
                                                   configures the internal replication logic to operate on the DB file in        
                                                   a manner that is compatible with litestream.                                  
                                                                                                                                 
     --storage-db-tmp-dir string                   optional                                                                      
       ZERO_STORAGE_DB_TMP_DIR env                                                                                               
                                                   tmp directory for IVM operator storage. Leave unset to use os.tmpdir()        
                                                                                                                                 
    "
  `);
});
