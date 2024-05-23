/**
 * Metadata, used for selective invalidation and catchup.
 *
 * These tables are created atomically in the setupReplicationTables migration step
 * after the logical replication handoff when initial data synchronization has completed.
 */
export const CREATE_INVALIDATION_TABLES =
  // Invalidation registry.
  //
  // * `id` of the NormalizedInvalidationFilterSpec as computed by
  //   {@link normalizeInvalidationFilterSpec}. This is a base36 encoded 64-bit value,
  //   which has a maximum length of 13 characters.
  //
  // * `spec` contains the InvalidationFilterSpec
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
  id                 VARCHAR(13) NOT NULL,
  spec               JSONB       NOT NULL,
  "fromStateVersion" VARCHAR(38) NOT NULL,
  "lastRequested"    TIMESTAMPTZ NOT NULL,
  CONSTRAINT "PK_InvalidationRegistry" PRIMARY KEY(id),
  CONSTRAINT "ID_InvalidationRegistry" CHECK (spec ->> 'id' = id)
);
` +
  // A btree over the InvalidationRegistry's "lastRequested" column allows
  // efficient deprecation of invalidation functions.
  `
CREATE INDEX "InvalidationRegistry_lastRequested_btree" 
  ON _zero."InvalidationRegistry" 
  USING BTREE ("lastRequested");
` +
  // A btree over the InvalidationRegistry's "fromStateVersion" column allows
  // efficient sorting of invalidation functions upon registration.
  `
CREATE INDEX "InvalidationRegistry_fromStateVersion_btree" 
  ON _zero."InvalidationRegistry" 
  USING BTREE ("fromStateVersion");
` +
  // A single-row table that tracks the "stateVersion" at which the last change
  // to the InvalidationRegistry's set of `spec`s happened. This is updated, for
  // example, when a new spec is added (with the value being equal to the new spec's
  // `fromStateVersion` column), or when specs are deleted for cleanup. The value
  // is NULL if there are no registered filters.
  //
  // The Invalidator caches this version along with the set of invalidation filter specs,
  // checking the version on every transaction to ensure that it's cache is consistent
  // with the state of the database. If the version has advanced, it reloads the specs
  // from the InvalidationRegistry table.
  //
  // Note: The `lock` column transparently ensures that at most one row exists.
  `
CREATE TABLE _zero."InvalidationRegistryVersion" (
  "stateVersionAtLastSpecChange" VARCHAR(38),

  lock char(1) NOT NULL CONSTRAINT "DF_InvalidationRegistryVersion" DEFAULT 'v',
  CONSTRAINT "PK_InvalidationRegistryVersion" PRIMARY KEY (lock),
  CONSTRAINT "CK_InvalidationRegistryVersion" CHECK (lock='v')
);
INSERT INTO _zero."InvalidationRegistryVersion" ("stateVersionAtLastSpecChange") VALUES (NULL);
` +
  // Invalidation index.
  //
  // * `hash` is the XXH64 hash of the invalidation tag produced by an invalidation function.
  // * `stateVersion` is the latest stateVersion in which the hash was produced.
  `
CREATE TABLE _zero."InvalidationIndex" (
  hash           BYTEA       NOT NULL,
  "stateVersion" VARCHAR(38) NOT NULL,
  CONSTRAINT "PK_InvalidationIndex" PRIMARY KEY(hash)
);
` +
  // A btree over the InvalidationIndex's "stateVersion" column allows
  // efficient `WHERE "stateVersion"` inequality conditions used for:
  // 1. Determining if newer-than-CVR hashes exist
  // 2. Cleaning up old hashes to keep storage usage in check
  `
CREATE INDEX "InvalidationIndex_stateVersion_btree" 
  ON _zero."InvalidationIndex" 
  USING BTREE ("stateVersion");
`;
