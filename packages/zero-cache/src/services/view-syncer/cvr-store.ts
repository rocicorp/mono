import {trace} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import {startAsyncSpan} from '../../../../otel/src/span.ts';
import {version} from '../../../../otel/src/version.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import {CustomKeySet} from '../../../../shared/src/custom-key-set.ts';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../shared/src/json.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import * as v from '../../../../shared/src/valita.ts';
import {astSchema} from '../../../../zero-protocol/src/ast.ts';
import {clientSchemaSchema} from '../../../../zero-protocol/src/client-schema.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import type {InspectQueryRow} from '../../../../zero-protocol/src/inspect-down.ts';
import {clampTTL, DEFAULT_TTL_MS} from '../../../../zql/src/query/ttl.ts';
import * as Mode from '../../db/mode-enum.ts';
import {TransactionPool} from '../../db/transaction-pool.ts';
import {recordRowsSynced} from '../../server/anonymous-otel-start.ts';
import {ProtocolErrorWithLevel} from '../../types/error-with-level.ts';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.ts';
import {rowIDString} from '../../types/row-key.ts';
import {cvrSchema, type ShardID} from '../../types/shards.ts';
import type {Patch, PatchToVersion} from './client-handler.ts';
import type {CVR, CVRSnapshot} from './cvr.ts';
import {RowRecordCache} from './row-record-cache.ts';
import {
  type ClientsRow,
  type DesiresRow,
  type InstancesRow,
  type QueriesRow,
  type RowsRow,
} from './schema/cvr.ts';
import {
  type ClientQueryRecord,
  type ClientRecord,
  cmpVersions,
  type CustomQueryRecord,
  type CVRVersion,
  EMPTY_CVR_VERSION,
  type InternalQueryRecord,
  type NullableCVRVersion,
  type QueryPatch,
  type QueryRecord,
  queryRecordToQueryRow,
  type RowID,
  type RowRecord,
  versionFromString,
  versionString,
} from './schema/types.ts';
import {
  type TTLClock,
  ttlClockAsNumber,
  ttlClockFromNumber,
} from './ttl-clock.ts';

export type CVRFlushStats = {
  instances: number;
  queries: number;
  desires: number;
  clients: number;
  rows: number;
  rowsDeferred: number;
  statements: number;
};

let flushCounter = 0;

const tracer = trace.getTracer('cvr-store', version);

function asQuery(row: QueriesRow): QueryRecord {
  const maybeVersion = (s: string | null) =>
    s === null ? undefined : versionFromString(s);

  if (row.clientAST === null) {
    // custom query
    assert(
      row.queryName !== null && row.queryArgs !== null,
      'queryName and queryArgs must be set for custom queries',
    );
    return {
      type: 'custom',
      id: row.queryHash,
      name: row.queryName,
      args: row.queryArgs,
      patchVersion: maybeVersion(row.patchVersion),
      clientState: {},
      transformationHash: row.transformationHash ?? undefined,
      transformationVersion: maybeVersion(row.transformationVersion),
    } satisfies CustomQueryRecord;
  }

  const ast = astSchema.parse(row.clientAST);
  return row.internal
    ? ({
        type: 'internal',
        id: row.queryHash,
        ast,
        transformationHash: row.transformationHash ?? undefined,
        transformationVersion: maybeVersion(row.transformationVersion),
      } satisfies InternalQueryRecord)
    : ({
        type: 'client',
        id: row.queryHash,
        ast,
        patchVersion: maybeVersion(row.patchVersion),
        clientState: {},
        transformationHash: row.transformationHash ?? undefined,
        transformationVersion: maybeVersion(row.transformationVersion),
      } satisfies ClientQueryRecord);
}

// The time to wait between load attempts.
const LOAD_ATTEMPT_INTERVAL_MS = 500;
// The maximum number of load() attempts if the rowsVersion is behind.
// This currently results in a maximum catchup time of ~5 seconds, after
// which we give up and consider the CVR invalid.
//
// TODO: Make this configurable with something like --max-catchup-wait-ms,
//       as it is technically application specific.
const MAX_LOAD_ATTEMPTS = 10;

// Type for pending desire upserts with pre-computed values for both old and new columns
type PendingDesireUpsert = {
  clientGroupID: string;
  clientID: string;
  queryHash: string;
  patchVersion: string;
  deleted: boolean;
  // Old columns (for backward compatibility)
  ttlInterval: number | null; // seconds
  inactivatedAtTimestamp: number | null; // seconds
  // New columns
  ttlMs: number | null;
  inactivatedAtMs: number | null;
};

// Type for pending query updates
type PendingQueryUpdate = {
  queryHash: string;
  patchVersion: string | null;
  transformationHash: string | null;
  transformationVersion: string | null;
  deleted: boolean;
};

// Type for pending query deletes (mark as deleted)
type PendingQueryDelete = {
  queryHash: string;
  patchVersion: string;
};

export class CVRStore {
  readonly #schema: string;
  readonly #taskID: string;
  readonly #id: string;
  readonly #failService: (e: unknown) => void;
  readonly #db: PostgresDB;

  // Batched write accumulators (replacing the old #writes Set)
  // Maps are used to de-duplicate by conflict key, keeping only the last value
  #pendingInstance:
    | (Pick<
        CVRSnapshot,
        | 'version'
        | 'replicaVersion'
        | 'lastActive'
        | 'clientSchema'
        | 'profileID'
        | 'ttlClock'
      > & {needsWrite: boolean})
    | undefined;
  // Key: queryHash
  readonly #pendingQueryInserts = new Map<string, QueriesRow>();
  // Key: queryHash
  readonly #pendingQueryDeletes = new Map<string, PendingQueryDelete>();
  // Key: queryHash
  readonly #pendingQueryUpdates = new Map<string, PendingQueryUpdate>();
  // Key: clientID
  readonly #pendingClientInserts = new Map<string, ClientsRow>();
  // Key: clientID
  readonly #pendingClientDeletes = new Set<string>();
  // Key: clientID + '|' + queryHash
  readonly #pendingDesireUpserts = new Map<string, PendingDesireUpsert>();

  readonly #pendingRowRecordUpdates = new CustomKeyMap<RowID, RowRecord | null>(
    rowIDString,
  );
  readonly #forceUpdates = new CustomKeySet<RowID>(rowIDString);
  readonly #rowCache: RowRecordCache;
  readonly #loadAttemptIntervalMs: number;
  readonly #maxLoadAttempts: number;
  #rowCount: number = 0;

  constructor(
    lc: LogContext,
    cvrDb: PostgresDB,
    shard: ShardID,
    taskID: string,
    cvrID: string,
    failService: (e: unknown) => void,
    loadAttemptIntervalMs = LOAD_ATTEMPT_INTERVAL_MS,
    maxLoadAttempts = MAX_LOAD_ATTEMPTS,
    deferredRowFlushThreshold = 100, // somewhat arbitrary
    setTimeoutFn = setTimeout,
  ) {
    this.#failService = failService;
    this.#db = cvrDb;
    this.#schema = cvrSchema(shard);
    this.#taskID = taskID;
    this.#id = cvrID;
    this.#rowCache = new RowRecordCache(
      lc,
      cvrDb,
      shard,
      cvrID,
      failService,
      deferredRowFlushThreshold,
      setTimeoutFn,
    );
    this.#loadAttemptIntervalMs = loadAttemptIntervalMs;
    this.#maxLoadAttempts = maxLoadAttempts;
  }

  #cvr(table: string) {
    return this.#db(`${this.#schema}.${table}`);
  }

  load(lc: LogContext, lastConnectTime: number): Promise<CVR> {
    return startAsyncSpan(tracer, 'cvr.load', async () => {
      let err: RowsVersionBehindError | undefined;
      for (let i = 0; i < this.#maxLoadAttempts; i++) {
        if (i > 0) {
          await sleep(this.#loadAttemptIntervalMs);
        }
        const result = await this.#load(lc, lastConnectTime);
        if (result instanceof RowsVersionBehindError) {
          lc.info?.(`attempt ${i + 1}: ${String(result)}`);
          err = result;
          continue;
        }
        return result;
      }
      assert(err, 'Expected error to be set after retry loop exhausted');
      throw new ClientNotFoundError(
        `max attempts exceeded waiting for CVR@${err.cvrVersion} to catch up from ${err.rowsVersion}`,
      );
    });
  }

  async #load(
    lc: LogContext,
    lastConnectTime: number,
  ): Promise<CVR | RowsVersionBehindError> {
    const start = Date.now();

    const id = this.#id;
    const cvr: CVR = {
      id,
      version: EMPTY_CVR_VERSION,
      lastActive: 0,
      ttlClock: ttlClockFromNumber(0), // TTL clock starts at 0, not Date.now()
      replicaVersion: null,
      clients: {},
      queries: {},
      clientSchema: null,
      profileID: null,
    };

    const [instance, clientsRows, queryRows, desiresRows] =
      await this.#db.begin(Mode.READONLY, tx => {
        lc.debug?.(`CVR tx started after ${Date.now() - start} ms`);
        return [
          tx<
            (Omit<InstancesRow, 'clientGroupID'> & {
              profileID: string | null;
              deleted: boolean;
              rowsVersion: string | null;
            })[]
          >`SELECT cvr."version",
                 "lastActive",
                 "ttlClock",
                 "replicaVersion",
                 "owner",
                 "grantedAt",
                 "clientSchema",
                 "profileID",
                 "deleted",
                 rows."version" as "rowsVersion"
            FROM ${this.#cvr('instances')} AS cvr
            LEFT JOIN ${this.#cvr('rowsVersion')} AS rows
            ON cvr."clientGroupID" = rows."clientGroupID"
            WHERE cvr."clientGroupID" = ${id}`,
          tx<Pick<ClientsRow, 'clientID'>[]>`SELECT "clientID" FROM ${this.#cvr(
            'clients',
          )}
           WHERE "clientGroupID" = ${id}`,
          tx<QueriesRow[]>`SELECT * FROM ${this.#cvr('queries')}
          WHERE "clientGroupID" = ${id} AND deleted IS DISTINCT FROM true`,
          tx<DesiresRow[]>`SELECT
          "clientGroupID",
          "clientID",
          "queryHash",
          "patchVersion",
          "deleted",
          "ttlMs" AS "ttl",
          "inactivatedAtMs" AS "inactivatedAt"
          FROM ${this.#cvr('desires')}
          WHERE "clientGroupID" = ${id}`,
        ];
      });
    lc.debug?.(
      `CVR tx completed after ${Date.now() - start} ms ` +
        `(${clientsRows.length} clients, ${queryRows.length} queries, ${desiresRows.length} desires)`,
    );

    if (instance.length === 0) {
      // This is the first time we see this CVR.
      this.putInstance({
        version: cvr.version,
        lastActive: 0,
        ttlClock: ttlClockFromNumber(0), // TTL clock starts at 0 for new instances
        replicaVersion: null,
        clientSchema: null,
        profileID: null,
      });
    } else {
      assert(
        instance.length === 1,
        () => `Expected exactly one CVR instance, got ${instance.length}`,
      );
      const {
        version,
        lastActive,
        ttlClock,
        replicaVersion,
        owner,
        grantedAt,
        rowsVersion,
        clientSchema,
        profileID,
        deleted,
      } = instance[0];

      if (deleted) {
        throw new ClientNotFoundError(
          'Client has been purged due to inactivity',
        );
      }

      if (owner !== this.#taskID) {
        if ((grantedAt ?? 0) > lastConnectTime) {
          throw new OwnershipError(owner, grantedAt, lastConnectTime);
        } else {
          // Fire-and-forget an ownership change to signal the current owner.
          // Note that the query is structured such that it only succeeds in the
          // correct conditions (i.e. gated on `grantedAt`).
          void this.#db`
            UPDATE ${this.#cvr('instances')} 
              SET "owner"     = ${this.#taskID}, 
                  "grantedAt" = ${lastConnectTime}
              WHERE "clientGroupID" = ${this.#id} AND
                    ("grantedAt" IS NULL OR
                     "grantedAt" <= to_timestamp(${lastConnectTime / 1000}))
        `
            .execute()
            .catch(this.#failService);
        }
      }

      if (version !== (rowsVersion ?? EMPTY_CVR_VERSION.stateVersion)) {
        // This will cause the load() method to wait for row catchup and retry.
        // Assuming the ownership signal succeeds, the current owner will stop
        // modifying the CVR and flush its pending row changes.
        return new RowsVersionBehindError(version, rowsVersion);
      }

      cvr.version = versionFromString(version);
      cvr.lastActive = lastActive;
      cvr.ttlClock = ttlClock;
      cvr.replicaVersion = replicaVersion;
      cvr.profileID = profileID;

      try {
        cvr.clientSchema =
          clientSchema === null
            ? null
            : v.parse(clientSchema, clientSchemaSchema);
      } catch (e) {
        throw new InvalidClientSchemaError(e);
      }
    }

    for (const row of clientsRows) {
      cvr.clients[row.clientID] = {
        id: row.clientID,
        desiredQueryIDs: [],
      };
    }

    for (const row of queryRows) {
      const query = asQuery(row);
      cvr.queries[row.queryHash] = query;
    }

    for (const row of desiresRows) {
      const client = cvr.clients[row.clientID];
      // Note: row.inactivatedAt is mapped from inactivatedAtMs in the SQL query
      if (!row.deleted && row.inactivatedAt === null) {
        if (client) {
          client.desiredQueryIDs.push(row.queryHash);
        } else {
          // This can happen if the client was deleted but the queries are still alive.
          lc.debug?.(
            `Not adding to desiredQueryIDs for client ${row.clientID} because it has been deleted.`,
          );
        }
      }

      const query = cvr.queries[row.queryHash];
      if (
        query &&
        query.type !== 'internal' &&
        (!row.deleted || row.inactivatedAt !== null)
      ) {
        query.clientState[row.clientID] = {
          inactivatedAt: row.inactivatedAt ?? undefined,
          ttl: clampTTL(row.ttl ?? DEFAULT_TTL_MS),
          version: versionFromString(row.patchVersion),
        };
      }
    }

    lc.debug?.(
      `loaded cvr@${versionString(cvr.version)} (${Date.now() - start} ms)`,
    );

    // why do we not sort `desiredQueryIDs` here?

    return cvr;
  }

  getRowRecords(): Promise<ReadonlyMap<RowID, RowRecord>> {
    return this.#rowCache.getRowRecords();
  }

  putRowRecord(row: RowRecord): void {
    this.#pendingRowRecordUpdates.set(row.id, row);
  }

  /**
   * Note: Removing a row from the CVR should be represented by a
   *       {@link putRowRecord()} with `refCounts: null` in order to properly
   *       produce the appropriate delete patch when catching up old clients.
   *
   * This `delRowRecord()` method, on the other hand, is only used for
   * "canceling" the put of a row that was not in the CVR in the first place.
   */
  delRowRecord(id: RowID): void {
    this.#pendingRowRecordUpdates.set(id, null);
  }

  /**
   * Overrides the default logic that removes no-op writes and forces
   * the updates for the given row `ids`. This has no effect if there
   * are no corresponding puts or dels for the associated row records.
   */
  forceUpdates(...ids: RowID[]) {
    for (const id of ids) {
      this.#forceUpdates.add(id);
    }
  }

  /**
   * Updates the `ttlClock` of the CVR instance. The ttlClock starts at 0 when
   * the CVR instance is first created and increments based on elapsed time
   * since the base time established by the ViewSyncerService.
   */
  async updateTTLClock(ttlClock: TTLClock, lastActive: number): Promise<void> {
    await this.#db`UPDATE ${this.#cvr('instances')}
          SET "lastActive" = ${lastActive},
              "ttlClock" = ${ttlClock}
          WHERE "clientGroupID" = ${this.#id}`.execute();
  }

  /**
   * @returns This returns the current `ttlClock` of the CVR instance. The ttlClock
   *          represents elapsed time since the instance was created (starting from 0).
   *          If the CVR has never been initialized for this client group, it returns
   *          `undefined`.
   */
  async getTTLClock(): Promise<TTLClock | undefined> {
    const result = await this.#db<Pick<InstancesRow, 'ttlClock'>[]>`
      SELECT "ttlClock" FROM ${this.#cvr('instances')}
      WHERE "clientGroupID" = ${this.#id}`.values();
    if (result.length === 0) {
      // This can happen if the CVR has not been initialized yet.
      return undefined;
    }
    assert(
      result.length === 1,
      () => `Expected exactly one rowsVersion result, got ${result.length}`,
    );
    return result[0][0];
  }

  putInstance({
    version,
    replicaVersion,
    lastActive,
    clientSchema,
    profileID,
    ttlClock,
  }: Pick<
    CVRSnapshot,
    | 'version'
    | 'replicaVersion'
    | 'lastActive'
    | 'clientSchema'
    | 'profileID'
    | 'ttlClock'
  >): void {
    this.#pendingInstance = {
      version,
      replicaVersion,
      lastActive,
      clientSchema,
      profileID,
      ttlClock,
      needsWrite: true,
    };
  }

  markQueryAsDeleted(version: CVRVersion, queryPatch: QueryPatch): void {
    this.#pendingQueryDeletes.set(queryPatch.id, {
      queryHash: queryPatch.id,
      patchVersion: versionString(version),
    });
  }

  putQuery(query: QueryRecord): void {
    const change: QueriesRow = queryRecordToQueryRow(this.#id, query);
    this.#pendingQueryInserts.set(query.id, change);
  }

  updateQuery(query: QueryRecord) {
    const maybeVersionString = (v: CVRVersion | undefined) =>
      v ? versionString(v) : null;

    this.#pendingQueryUpdates.set(query.id, {
      queryHash: query.id,
      patchVersion:
        query.type === 'internal'
          ? null
          : maybeVersionString(query.patchVersion),
      transformationHash: query.transformationHash ?? null,
      transformationVersion: maybeVersionString(query.transformationVersion),
      deleted: false,
    });
  }

  insertClient(client: ClientRecord): void {
    this.#pendingClientInserts.set(client.id, {
      clientGroupID: this.#id,
      clientID: client.id,
    });
  }

  deleteClient(clientID: string) {
    this.#pendingClientDeletes.add(clientID);
  }

  putDesiredQuery(
    newVersion: CVRVersion,
    query: {id: string},
    client: {id: string},
    deleted: boolean,
    inactivatedAt: TTLClock | undefined,
    ttl: number,
  ): void {
    // For backward compatibility during rollout, write to both old and new columns:
    // Old columns: inactivatedAt (TIMESTAMPTZ), ttl (INTERVAL) - need conversion ms->seconds
    // New columns: inactivatedAtMs (DOUBLE PRECISION), ttlMs (DOUBLE PRECISION) - store ms directly (1:1 with JS)
    const inactivatedAtTimestamp =
      inactivatedAt === undefined
        ? null
        : ttlClockAsNumber(
            ttlClockFromNumber(ttlClockAsNumber(inactivatedAt) / 1000),
          );
    const inactivatedAtMs =
      inactivatedAt !== undefined ? ttlClockAsNumber(inactivatedAt) : null;
    const ttlInterval = ttl < 0 ? null : ttl / 1000; // INTERVAL needs seconds
    const ttlMs = ttl < 0 ? null : ttl; // New column stores ms directly

    const key = `${client.id}|${query.id}`;
    this.#pendingDesireUpserts.set(key, {
      clientGroupID: this.#id,
      clientID: client.id,
      queryHash: query.id,
      patchVersion: versionString(newVersion),
      deleted,
      ttlInterval,
      inactivatedAtTimestamp,
      ttlMs,
      inactivatedAtMs,
    });
  }

  catchupRowPatches(
    lc: LogContext,
    afterVersion: NullableCVRVersion,
    upToCVR: CVRSnapshot,
    current: CVRVersion,
    excludeQueryHashes: string[] = [],
  ): AsyncGenerator<RowsRow[], void, undefined> {
    return this.#rowCache.catchupRowPatches(
      lc,
      afterVersion,
      upToCVR,
      current,
      excludeQueryHashes,
    );
  }

  async catchupConfigPatches(
    lc: LogContext,
    afterVersion: NullableCVRVersion,
    upToCVR: CVRSnapshot,
    current: CVRVersion,
  ): Promise<PatchToVersion[]> {
    if (cmpVersions(afterVersion, upToCVR.version) >= 0) {
      return [];
    }

    const startMs = Date.now();
    const start = afterVersion ? versionString(afterVersion) : '';
    const end = versionString(upToCVR.version);
    lc.debug?.(`scanning config patches for clients from ${start}`);

    const reader = new TransactionPool(lc, Mode.READONLY).run(this.#db);
    try {
      // Verify that we are reading the right version of the CVR.
      await reader.processReadTask(tx =>
        checkVersion(tx, this.#schema, this.#id, current),
      );

      const [allDesires, queryRows] = await reader.processReadTask(tx =>
        Promise.all([
          tx<DesiresRow[]>`
      SELECT * FROM ${this.#cvr('desires')}
        WHERE "clientGroupID" = ${this.#id}
        AND "patchVersion" > ${start}
        AND "patchVersion" <= ${end}`,
          tx<Pick<QueriesRow, 'deleted' | 'queryHash' | 'patchVersion'>[]>`
      SELECT deleted, "queryHash", "patchVersion" FROM ${this.#cvr('queries')}
        WHERE "clientGroupID" = ${this.#id}
        AND "patchVersion" > ${start}
        AND "patchVersion" <= ${end}`,
        ]),
      );

      const patches: PatchToVersion[] = [];
      for (const row of queryRows) {
        const {queryHash: id} = row;
        const patch: Patch = row.deleted
          ? {type: 'query', op: 'del', id}
          : {type: 'query', op: 'put', id};
        const v = row.patchVersion;
        assert(v, 'patchVersion must be set for query patches');
        patches.push({patch, toVersion: versionFromString(v)});
      }
      for (const row of allDesires) {
        const {clientID, queryHash: id} = row;
        const patch: Patch = row.deleted
          ? {type: 'query', op: 'del', id, clientID}
          : {type: 'query', op: 'put', id, clientID};
        patches.push({patch, toVersion: versionFromString(row.patchVersion)});
      }

      lc.debug?.(
        `${patches.length} config patches (${Date.now() - startMs} ms)`,
      );
      return patches;
    } finally {
      reader.setDone();
    }
  }

  async #checkVersionAndOwnership(
    lc: LogContext,
    tx: PostgresTransaction,
    expectedCurrentVersion: CVRVersion,
    lastConnectTime: number,
  ): Promise<void> {
    const start = Date.now();
    lc.debug?.('checking cvr version and ownership');
    const expected = versionString(expectedCurrentVersion);
    const result = await tx<
      Pick<InstancesRow, 'version' | 'owner' | 'grantedAt'>[]
    >`SELECT "version", "owner", "grantedAt" FROM ${this.#cvr('instances')}
        WHERE "clientGroupID" = ${this.#id}
        FOR UPDATE`.execute(); // Note: execute() immediately to send the query before others.
    const {version, owner, grantedAt} =
      result.length > 0
        ? result[0]
        : {
            version: EMPTY_CVR_VERSION.stateVersion,
            owner: null,
            grantedAt: null,
          };
    lc.debug?.(
      'checked cvr version and ownership in ' + (Date.now() - start) + ' ms',
    );
    if (owner !== this.#taskID && (grantedAt ?? 0) > lastConnectTime) {
      throw new OwnershipError(owner, grantedAt, lastConnectTime);
    }
    if (version !== expected) {
      throw new ConcurrentModificationException(expected, version);
    }
  }

  #hasPendingWrites(): boolean {
    return !!(
      this.#pendingInstance?.needsWrite ||
      this.#pendingQueryInserts.size ||
      this.#pendingQueryDeletes.size ||
      this.#pendingQueryUpdates.size ||
      this.#pendingClientInserts.size ||
      this.#pendingClientDeletes.size ||
      this.#pendingDesireUpserts.size
    );
  }

  #clearPendingWrites(): void {
    this.#pendingInstance = undefined;
    this.#pendingQueryInserts.clear();
    this.#pendingQueryDeletes.clear();
    this.#pendingQueryUpdates.clear();
    this.#pendingClientInserts.clear();
    this.#pendingClientDeletes.clear();
    this.#pendingDesireUpserts.clear();
  }

  async #flush(
    lc: LogContext,
    expectedCurrentVersion: CVRVersion,
    cvr: CVRSnapshot,
    lastConnectTime: number,
  ): Promise<CVRFlushStats | null> {
    const stats: CVRFlushStats = {
      instances: 0,
      queries: 0,
      desires: 0,
      clients: 0,
      rows: 0,
      rowsDeferred: 0,
      statements: 0,
    };
    if (this.#pendingRowRecordUpdates.size) {
      const existingRowRecords = await this.getRowRecords();
      this.#rowCount = existingRowRecords.size;
      for (const [id, row] of this.#pendingRowRecordUpdates.entries()) {
        if (this.#forceUpdates.has(id)) {
          continue;
        }
        const existing = existingRowRecords.get(id);
        if (
          // Don't delete or add an unreferenced row if it's not in the CVR.
          (existing === undefined && !row?.refCounts) ||
          // Don't write a row record that exactly matches what's in the CVR.
          deepEqual(
            (row ?? undefined) as ReadonlyJSONValue | undefined,
            existing as ReadonlyJSONValue | undefined,
          )
        ) {
          this.#pendingRowRecordUpdates.delete(id);
        }
      }
    }
    if (this.#pendingRowRecordUpdates.size === 0 && !this.#hasPendingWrites()) {
      return null;
    }
    // Note: The CVR instance itself is only updated if there are material
    // changes (i.e. changes to the CVR contents) to flush.
    this.putInstance(cvr);
    const start = Date.now();
    lc.debug?.('flush tx beginning');
    const rowsFlushed = await this.#db.begin(Mode.READ_COMMITTED, async tx => {
      lc.debug?.(`flush tx begun after ${Date.now() - start} ms`);
      const pipelined: Promise<unknown>[] = [
        // #checkVersionAndOwnership() executes a `SELECT ... FOR UPDATE`
        // query to acquire a row-level lock so that version-updating
        // transactions are effectively serialized per cvr.instance.
        //
        // Note that `rowsVersion` updates, on the other hand, are not subject
        // to this lock and can thus commit / be-committed independently of
        // cvr.instances.
        this.#checkVersionAndOwnership(
          lc,
          tx,
          expectedCurrentVersion,
          lastConnectTime,
        ),
      ];

      // Batch instance upsert (typically 1)
      if (this.#pendingInstance?.needsWrite) {
        const inst = this.#pendingInstance;
        const change: InstancesRow = {
          clientGroupID: this.#id,
          version: versionString(inst.version),
          lastActive: inst.lastActive,
          ttlClock: inst.ttlClock,
          replicaVersion: inst.replicaVersion,
          owner: this.#taskID,
          grantedAt: lastConnectTime,
          clientSchema: inst.clientSchema,
          profileID: inst.profileID,
        };
        pipelined.push(
          tx`INSERT INTO ${this.#cvr('instances')} ${tx(change)}
            ON CONFLICT ("clientGroupID") DO UPDATE SET ${tx(change)}`.execute(),
        );
        stats.instances = 1;
        stats.statements++;
      }

      // Batch query inserts/upserts using json_to_recordset
      // This avoids issues with postgres.js array parameter handling
      if (this.#pendingQueryInserts.size > 0) {
        const rows = Array.from(this.#pendingQueryInserts.values());
        // Pre-process rows to handle queryArgs JSON serialization
        const jsonRows = rows.map(r => ({
          clientGroupID: r.clientGroupID,
          queryHash: r.queryHash,
          clientAST: r.clientAST,
          queryName: r.queryName,
          queryArgs: r.queryArgs,
          patchVersion: r.patchVersion,
          transformationHash: r.transformationHash ?? null,
          transformationVersion: r.transformationVersion ?? null,
          internal: r.internal,
          deleted: r.deleted ?? false,
        }));
        pipelined.push(
          tx`
          INSERT INTO ${this.#cvr('queries')} (
            "clientGroupID", "queryHash", "clientAST", "queryName", "queryArgs",
            "patchVersion", "transformationHash", "transformationVersion", "internal", "deleted"
          )
          SELECT
            "clientGroupID", "queryHash", "clientAST", "queryName", "queryArgs",
            "patchVersion", "transformationHash", "transformationVersion", "internal", "deleted"
          FROM jsonb_to_recordset(${jsonRows}::jsonb) AS x(
            "clientGroupID" text,
            "queryHash" text,
            "clientAST" jsonb,
            "queryName" text,
            "queryArgs" jsonb,
            "patchVersion" text,
            "transformationHash" text,
            "transformationVersion" text,
            "internal" boolean,
            "deleted" boolean
          )
          ON CONFLICT ("clientGroupID", "queryHash")
          DO UPDATE SET
            "clientAST" = EXCLUDED."clientAST",
            "queryName" = EXCLUDED."queryName",
            "queryArgs" = EXCLUDED."queryArgs",
            "patchVersion" = EXCLUDED."patchVersion",
            "transformationHash" = EXCLUDED."transformationHash",
            "transformationVersion" = EXCLUDED."transformationVersion",
            "internal" = EXCLUDED."internal",
            "deleted" = EXCLUDED."deleted"
          `.execute(),
        );
        stats.queries += rows.length;
        stats.statements++;
      }

      // Batch query deletes (mark as deleted)
      if (this.#pendingQueryDeletes.size > 0) {
        const rows = Array.from(this.#pendingQueryDeletes.values());
        pipelined.push(
          tx`
          UPDATE ${this.#cvr('queries')} AS q SET
            "patchVersion" = u."patchVersion",
            "deleted" = true,
            "transformationHash" = NULL,
            "transformationVersion" = NULL
          FROM jsonb_to_recordset(${rows}::jsonb) AS u(
            "queryHash" text,
            "patchVersion" text
          )
          WHERE q."clientGroupID" = ${this.#id}
            AND q."queryHash" = u."queryHash"
          `.execute(),
        );
        stats.queries += rows.length;
        stats.statements++;
      }

      // Batch query updates
      if (this.#pendingQueryUpdates.size > 0) {
        const rows = Array.from(this.#pendingQueryUpdates.values());
        pipelined.push(
          tx`
          UPDATE ${this.#cvr('queries')} AS q SET
            "patchVersion" = u."patchVersion",
            "transformationHash" = u."transformationHash",
            "transformationVersion" = u."transformationVersion",
            "deleted" = u."deleted"
          FROM jsonb_to_recordset(${rows}::jsonb) AS u(
            "queryHash" text,
            "patchVersion" text,
            "transformationHash" text,
            "transformationVersion" text,
            "deleted" boolean
          )
          WHERE q."clientGroupID" = ${this.#id}
            AND q."queryHash" = u."queryHash"
          `.execute(),
        );
        stats.queries += rows.length;
        stats.statements++;
      }

      // Batch client inserts
      if (this.#pendingClientInserts.size > 0) {
        const clientRows = Array.from(this.#pendingClientInserts.values());
        pipelined.push(
          tx`INSERT INTO ${this.#cvr('clients')} ${tx(clientRows)}`.execute(),
        );
        stats.clients += clientRows.length;
        stats.statements++;
      }

      // Batch client deletes
      if (this.#pendingClientDeletes.size > 0) {
        const clientIDs = Array.from(this.#pendingClientDeletes);
        pipelined.push(
          tx`DELETE FROM ${this.#cvr('clients')}
            WHERE "clientGroupID" = ${this.#id}
              AND "clientID" IN ${tx(clientIDs)}`.execute(),
        );
        stats.clients += clientIDs.length;
        stats.statements++;
      }

      // Batch desire upserts
      if (this.#pendingDesireUpserts.size > 0) {
        const rows = Array.from(this.#pendingDesireUpserts.values());
        pipelined.push(
          tx`
          INSERT INTO ${this.#cvr('desires')} (
            "clientGroupID", "clientID", "queryHash", "patchVersion", "deleted",
            "ttl", "ttlMs", "inactivatedAt", "inactivatedAtMs"
          )
          SELECT
            "clientGroupID", "clientID", "queryHash", "patchVersion", "deleted",
            make_interval(secs => "ttlInterval"),
            "ttlMs",
            to_timestamp("inactivatedAtTimestamp"),
            "inactivatedAtMs"
          FROM jsonb_to_recordset(${rows}::jsonb) AS x(
            "clientGroupID" text,
            "clientID" text,
            "queryHash" text,
            "patchVersion" text,
            "deleted" boolean,
            "ttlInterval" double precision,
            "ttlMs" double precision,
            "inactivatedAtTimestamp" double precision,
            "inactivatedAtMs" double precision
          )
          ON CONFLICT ("clientGroupID", "clientID", "queryHash")
          DO UPDATE SET
            "patchVersion" = EXCLUDED."patchVersion",
            "deleted" = EXCLUDED."deleted",
            "ttl" = EXCLUDED."ttl",
            "ttlMs" = EXCLUDED."ttlMs",
            "inactivatedAt" = EXCLUDED."inactivatedAt",
            "inactivatedAtMs" = EXCLUDED."inactivatedAtMs"
          `.execute(),
        );
        stats.desires += rows.length;
        stats.statements++;
      }

      const rowUpdates = this.#rowCache.executeRowUpdates(
        tx,
        cvr.version,
        this.#pendingRowRecordUpdates,
        'allow-defer',
        lc,
      );
      pipelined.push(...rowUpdates);
      stats.statements += rowUpdates.length;

      // Make sure Errors thrown by pipelined statements
      // are propagated up the stack.
      await Promise.all(pipelined);
      lc.debug?.(`flush tx returning after ${Date.now() - start} ms`);
      if (rowUpdates.length === 0) {
        stats.rowsDeferred = this.#pendingRowRecordUpdates.size;
        return false;
      }
      stats.rows += this.#pendingRowRecordUpdates.size;

      return true;
    });

    this.#rowCount = await this.#rowCache.apply(
      this.#pendingRowRecordUpdates,
      cvr.version,
      rowsFlushed,
    );
    recordRowsSynced(this.#rowCount);

    return stats;
  }

  get rowCount(): number {
    return this.#rowCount;
  }

  async flush(
    lc: LogContext,
    expectedCurrentVersion: CVRVersion,
    cvr: CVRSnapshot,
    lastConnectTime: number,
  ): Promise<CVRFlushStats | null> {
    const start = performance.now();
    lc = lc.withContext('cvrFlushID', flushCounter++);
    try {
      const stats = await this.#flush(
        lc,
        expectedCurrentVersion,
        cvr,
        lastConnectTime,
      );
      if (stats) {
        const elapsed = performance.now() - start;
        lc.debug?.(
          `flushed cvr@${versionString(cvr.version)} ` +
            `${JSON.stringify(stats)} in (${elapsed} ms)`,
        );
        this.#rowCache.recordSyncFlushStats(stats, elapsed);
      }
      return stats;
    } catch (e) {
      // Clear cached state if an error (e.g. ConcurrentModificationException) is encountered.
      this.#rowCache.clear();
      throw e;
    } finally {
      this.#clearPendingWrites();
      this.#pendingRowRecordUpdates.clear();
      this.#forceUpdates.clear();
    }
  }

  hasPendingUpdates(): boolean {
    return this.#rowCache.hasPendingUpdates();
  }

  /** Resolves when all pending updates are flushed. */
  flushed(lc: LogContext): Promise<void> {
    return this.#rowCache.flushed(lc);
  }

  async inspectQueries(
    lc: LogContext,
    ttlClock: TTLClock,
    clientID?: string,
  ): Promise<InspectQueryRow[]> {
    const db = this.#db;
    const clientGroupID = this.#id;

    const reader = new TransactionPool(lc, Mode.READONLY).run(db);
    try {
      return await reader.processReadTask(
        tx => tx<InspectQueryRow[]>`
  SELECT DISTINCT ON (d."clientID", d."queryHash")
    d."clientID",
    d."queryHash" AS "queryID",
    COALESCE(d."ttlMs", ${DEFAULT_TTL_MS}) AS "ttl",
    d."inactivatedAtMs" AS "inactivatedAt",
    (SELECT COUNT(*)::INT FROM ${this.#cvr('rows')} r 
     WHERE r."clientGroupID" = d."clientGroupID" 
     AND r."refCounts" ? d."queryHash") AS "rowCount",
    q."clientAST" AS "ast",
    (q."patchVersion" IS NOT NULL) AS "got",
    COALESCE(d."deleted", FALSE) AS "deleted",
    q."queryName" AS "name",
    q."queryArgs" AS "args"
  FROM ${this.#cvr('desires')} d
  LEFT JOIN ${this.#cvr('queries')} q
    ON q."clientGroupID" = d."clientGroupID"
   AND q."queryHash" = d."queryHash"
  WHERE d."clientGroupID" = ${clientGroupID}
    ${clientID ? tx`AND d."clientID" = ${clientID}` : tx``}
    AND NOT (
      d."inactivatedAtMs" IS NOT NULL 
      AND d."ttlMs" IS NOT NULL 
      AND (d."inactivatedAtMs" + d."ttlMs") <= ${ttlClockAsNumber(ttlClock)}
    )
  ORDER BY d."clientID", d."queryHash"`,
      );
    } finally {
      reader.setDone();
    }
  }
}

/**
 * This is similar to {@link CVRStore.#checkVersionAndOwnership} except
 * that it only checks the version and is suitable for snapshot reads
 * (i.e. by doing a plain `SELECT` rather than a `SELECT ... FOR UPDATE`).
 */
export async function checkVersion(
  tx: PostgresTransaction,
  schema: string,
  clientGroupID: string,
  expectedCurrentVersion: CVRVersion,
): Promise<void> {
  const expected = versionString(expectedCurrentVersion);
  const result = await tx<Pick<InstancesRow, 'version'>[]>`
    SELECT version FROM ${tx(schema)}.instances 
      WHERE "clientGroupID" = ${clientGroupID}`;
  const {version} =
    result.length > 0 ? result[0] : {version: EMPTY_CVR_VERSION.stateVersion};
  if (version !== expected) {
    throw new ConcurrentModificationException(expected, version);
  }
}

export class ClientNotFoundError extends ProtocolErrorWithLevel {
  constructor(message: string) {
    super(
      {
        kind: ErrorKind.ClientNotFound,
        message,
        origin: ErrorOrigin.ZeroCache,
      },
      'warn',
    );
  }
}

export class ConcurrentModificationException extends ProtocolErrorWithLevel {
  readonly name = 'ConcurrentModificationException';

  constructor(expectedVersion: string, actualVersion: string) {
    super(
      {
        kind: ErrorKind.Internal,
        message: `CVR has been concurrently modified. Expected ${expectedVersion}, got ${actualVersion}`,
        origin: ErrorOrigin.ZeroCache,
      },
      'warn',
    );
  }
}

export class OwnershipError extends ProtocolErrorWithLevel {
  readonly name = 'OwnershipError';

  constructor(
    owner: string | null,
    grantedAt: number | null,
    lastConnectTime: number,
  ) {
    super(
      {
        kind: ErrorKind.Rehome,
        message:
          `CVR ownership was transferred to ${owner} at ` +
          `${new Date(grantedAt ?? 0).toISOString()} ` +
          `(last connect time: ${new Date(lastConnectTime).toISOString()})`,
        maxBackoffMs: 0,
        origin: ErrorOrigin.ZeroCache,
      },
      'info',
    );
  }
}

export class InvalidClientSchemaError extends ProtocolErrorWithLevel {
  readonly name = 'InvalidClientSchemaError';

  constructor(cause: unknown) {
    super(
      {
        kind: ErrorKind.SchemaVersionNotSupported,
        message: `Could not parse clientSchema stored in CVR: ${String(cause)}`,
        origin: ErrorOrigin.ZeroCache,
      },
      'warn',
      {cause},
    );
  }
}

export class RowsVersionBehindError extends Error {
  readonly name = 'RowsVersionBehindError';
  readonly cvrVersion: string;
  readonly rowsVersion: string | null;

  constructor(cvrVersion: string, rowsVersion: string | null) {
    super(`rowsVersion (${rowsVersion}) is behind CVR ${cvrVersion}`);
    this.cvrVersion = cvrVersion;
    this.rowsVersion = rowsVersion;
  }
}
