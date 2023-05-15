import type {WriteTransaction} from 'replicache';

/**
 * The `RoomStartHandler` is invoked when the room DurableObject instance
 * is started, before any connections are accepted. This is useful for
 * initializing or migrating room state.
 *
 * Note that the DurableObject instance may be started and shutdown multiple
 * times during the lifetime of a room. A succeeding RoomStartHandler (i.e.
 * no error thrown) is guaranteed to be invoked exactly once during the lifetime
 * of the DurableObject.
 *
 * If the RoomStartHandler throws an error, it will be retried on the next
 * connection attempt. Connections will continue to fail until the RoomStartHandler
 * succeeds.
 *
 * As the transaction is not associated with any client, `write.clientID`
 * will be empty and `write.mutationID` will be -1.
 *
 * TODO: Determine if there is a need to provide an API for incremental
 * migrations (i.e. multiple flushes).
 */
export type RoomStartHandler = (write: WriteTransaction) => Promise<void>;
