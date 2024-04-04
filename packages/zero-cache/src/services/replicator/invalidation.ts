import type {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {stringify} from 'json-custom-numbers';
import type postgres from 'postgres';
import {
  parseFilterSpec,
  type InvalidationFilterSpec,
  type NormalizedInvalidationFilterSpec,
} from '../../types/invalidation.js';
import type {LexiVersion} from '../../types/lexi-version.js';
import type {
  RegisterInvalidationFiltersRequest,
  RegisterInvalidationFiltersResponse,
} from './replicator.js';

/**
 * Metadata, used for selective invalidation and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables} after
 * the logical replication handoff when initial data synchronization has completed.
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
  // `fromStateVersion` column), or when specs are deleted for cleanup.
  //
  // The Invalidator caches this version along with the set of invalidation filter specs,
  // checking the version on every transaction to ensure that it's cache is consistent
  // with the state of the database. If the version has advanced, it reloads the specs
  // from the InvalidationRegistry table.
  //
  // Note: The `lock` column transparently ensures that at most one row exists.
  `
CREATE TABLE _zero."InvalidationRegistryVersion" (
  "stateVersionAtLastSpecChange" VARCHAR(38) NOT NULL,

  lock char(1) NOT NULL CONSTRAINT "DF_InvalidationRegistryVersion" DEFAULT 'v',
  CONSTRAINT "PK_InvalidationRegistryVersion" PRIMARY KEY (lock),
  CONSTRAINT "CK_InvalidationRegistryVersion" CHECK (lock='v')
);
` +
  // Invalidation index.
  //
  // * `hash` is the XXH64 hash of the invalidation tag produced by an invalidation function.
  // * `stateVersion` is the latest stateVersion in which the hash was produced.
  `
CREATE TABLE _zero."InvalidationIndex" (
  hash           BYTEA       NOT NULL,
  "stateVersion" VARCHAR(38) NOT NULL,
  PRIMARY KEY(hash)
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

type CachedFilters = {
  specs: InvalidationFilterSpec[];
  version: LexiVersion;
};

export class Invalidator {
  readonly #replica: postgres.Sql;
  readonly #txSerializer: Lock;
  readonly #lastRequestedTimes = new Map<string, Date>();

  // Versioned cache of the InvalidationRegistry.
  cachedFilters: CachedFilters | undefined;

  constructor(replica: postgres.Sql, txSerializer: Lock) {
    this.#replica = replica;
    this.#txSerializer = txSerializer;
  }

  async registerInvalidationFilters(
    lc: LogContext,
    req: RegisterInvalidationFiltersRequest,
    now = new Date(),
  ): Promise<RegisterInvalidationFiltersResponse> {
    const specsByID = new Map<string, NormalizedInvalidationFilterSpec>();
    req.specs.forEach(spec => specsByID.set(spec.id, spec));
    if (!specsByID.size) {
      throw new Error(`No specs specified in ${stringify(req)}`);
    }

    // TODO: When spec cleanup is implemented, guarantee that these specs won't disappear.
    for (const id of specsByID.keys()) {
      this.#lastRequestedTimes.set(id, now);
    }

    const values = [...specsByID.keys()].map(id => this.#replica`${id}`);
    const getVersions = (db: postgres.Sql) => db<
      {id: string; fromStateVersion: LexiVersion | null}[]
    >`
    WITH ids (id) AS (VALUES (${values.flatMap((id, i) =>
      i ? [db`),(`, id] : id,
    )}))
      SELECT id, "fromStateVersion" FROM ids
      LEFT JOIN _zero."InvalidationRegistry" USING (id)
      ORDER BY "fromStateVersion";
  `;

    const versions = await getVersions(this.#replica);
    const latest = versions[versions.length - 1].fromStateVersion;
    if (latest) {
      // Common case: All specs are already registered. Return the latest version.
      return {invalidatingFromVersion: latest};
    }

    // Register the specs from within the txSerializer.
    return this.#txSerializer.withLock(() =>
      this.#replica.begin(async tx => {
        // Check again in case registration happened while waiting for the lock
        // (e.g. a concurrent request).
        const versions = await getVersions(tx);
        const latest = versions[versions.length - 1].fromStateVersion;
        if (latest) {
          return {invalidatingFromVersion: latest};
        }

        // Get the current stateVersion.
        const stateVersion = await tx<{max: LexiVersion | null}[]>`
        SELECT MAX("stateVersion") FROM _zero."TxLog";`;
        const fromStateVersion = stateVersion[0].max ?? '00';

        const unregistered = versions.filter(
          row => row.fromStateVersion === null,
        );
        for (const {id} of unregistered) {
          const spec = specsByID.get(id);
          const row = {id, spec, fromStateVersion, lastRequested: now};
          lc.info?.(`Registering InvalidationFilterSpec`, spec);
          void tx`
          INSERT INTO _zero."InvalidationRegistry" ${tx(row)}
          `.execute();
        }

        // UPSERT the latest version into the InvalidationRegistryVersion.
        void tx`
        INSERT INTO _zero."InvalidationRegistryVersion" ${tx({
          stateVersionAtLastSpecChange: fromStateVersion,
        })}
          ON CONFLICT ON CONSTRAINT "PK_InvalidationRegistryVersion"
          DO UPDATE SET "stateVersionAtLastSpecChange" = EXCLUDED."stateVersionAtLastSpecChange";
          `.execute();

        await this.#ensureCachedFilters(lc, tx, fromStateVersion);

        return {invalidatingFromVersion: fromStateVersion};
      }),
    );
  }

  /**
   * Refreshes the CachedFilters, called whenever the set of specs is know to have changed.
   *
   * @param expectedVersion The expected version as read from the database. This is checked
   *        against any existing CachedFilters to see if they need to be reloaded. If unset,
   *        cached filters are loaded if not yet loaded.
   */
  async #ensureCachedFilters(
    lc: LogContext,
    db: postgres.Sql,
    expectedVersion?: LexiVersion,
  ): Promise<CachedFilters> {
    const cached = this.cachedFilters;
    if (cached && cached.version === (expectedVersion ?? cached.version)) {
      return cached;
    }
    const results = await db`
    SELECT spec FROM _zero."InvalidationRegistry";
    SELECT "stateVersionAtLastSpecChange" FROM _zero."InvalidationRegistryVersion";
    `.simple();

    const loaded: CachedFilters = {
      specs: results[0].map((row: {spec: unknown}) =>
        parseFilterSpec(row.spec),
      ),
      version: results[1].length
        ? results[1][0].stateVersionAtLastSpecChange
        : '00',
    };
    lc.info?.(
      `Loaded ${loaded.specs.length} filters at version ${loaded.version}`,
    );
    this.cachedFilters = loaded;
    return loaded;
  }
}
