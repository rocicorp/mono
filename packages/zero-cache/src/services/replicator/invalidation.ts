/**
 * Metadata, used for selective invalidation and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables} after
 * the logical replication handoff when initial data synchronization has completed.
 */
export const CREATE_INVALIDATION_TABLES =
  // Invalidation registry.
  //
  // * `spec` defines the invalidation function to run,
  //
  // * `bits` indicates the number of bits used to create the
  //    corresponding hash in the `invalidation_index`. The 'spec' is requested
  //    by View Syncers, while 'bits' is decided by the system.
  //
  //    For example, we may decide to start off with 32-bit hashes and later
  //    determine that it is worth increasing the table size to 40-bit hashes
  //    in order to reduce the number of collisions. During the transition, the
  //    Replicator would compute both sizes until the new size has sufficient
  //    coverage (over old versions).
  //
  // * `fromStateVersion` indicates when the Replicator first started running
  //   the filter. CVRs at or newer than the version are considered covered.
  //
  // * `lastRequested` records (approximately) the last time the spec was
  //   requested. This is not exact. It may only be updated if the difference
  //   exceeds some interval, for example. This is used to clean up specs that
  //   are no longer used.
  `
CREATE TABLE _zero."InvalidationRegistry" (
  spec               TEXT        NOT NULL,
  bits               SMALLINT    NOT NULL,
  "fromStateVersion" VARCHAR(38) NOT NULL,
  "lastRequested"    TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(spec, bits)
);
` +
  // Invalidation index.
  `
CREATE TABLE _zero."InvalidationIndex" (
  hash           BIGINT      NOT NULL,
  "stateVersion" VARCHAR(38) NOT NULL,
  PRIMARY KEY(hash)
);
`;
