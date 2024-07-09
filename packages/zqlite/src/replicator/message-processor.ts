/**
 * Handles incoming messages from the replicator.
 * Applies them to SQLite.
 * Commits the transaction once a boundary is reached, unless
 * IVM pipelines are still processing. In that case, continues
 * processing new writes until all pipelines are done and we reach a commit boundary.
 *
 * Tells IVM pipelines to process once tx boundary is reached and committed.
 */
export class MessageProcessor {}
