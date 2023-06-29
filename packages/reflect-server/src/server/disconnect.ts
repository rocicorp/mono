import type {WriteTransaction} from 'reflect-types/src/mod.js';

/**
 * A `DisconnectHandler` can modify room state in response to a client
 * disconnecting from the room.  These changes will be synced to all
 * clients of the room just like mutator changes.
 * `write.clientID` will be the id of the disconnected client.
 * `write.mutationID` will be -1.
 */
export type DisconnectHandler = (write: WriteTransaction) => Promise<void>;
