import type {WriteTransaction} from 'reflect-types/src/mod.js';

/**
 * The `RoomStartHandler` is invoked when the room is started, before
 * any connections are accepted. This is useful for initializing or migrating
 * room state.
 *
 * Note that rooms may be shutdown when idle and restarted when connections
 * resume, upon which the `RoomStartHandler` will be invoked again. It can
 * therefore conceivably be used for running logic when a room resumes after
 * being idle; however, there are no guarantees of timing with respect to idleness.
 *
 * A succeeding RoomStartHandler (i.e. no error thrown) is guaranteed to be
 * invoked exactly once during the lifetime of a room (from start to shutdown).
 *
 * If the RoomStartHandler throws an error, it will be retried on the next
 * connection attempt. Connections will continue to fail until the RoomStartHandler
 * succeeds.
 *
 * As the transaction is not associated with any client, `write.clientID`
 * will be empty and `write.mutationID` will be -1.
 */
export type RoomStartHandler = (write: WriteTransaction) => Promise<void>;
