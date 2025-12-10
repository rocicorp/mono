import {trace} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import type {MaybeRow, PendingQuery} from 'postgres';
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
import {cvrSchema, type ShardID, upstreamSchema} from '../../types/shards.ts';
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
      errorMessage: row.errorMessage ?? undefined,
      errorVersion: maybeVersion(row.errorVersion),
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
        errorMessage: row.errorMessage ?? undefined,
        errorVersion: maybeVersion(row.errorVersion),
      } satisfies InternalQueryRecord)
    : ({
        type: 'client',
        id: row.queryHash,
        ast,
        patchVersion: maybeVersion(row.patchVersion),
        clientState: {},
        transformationHash: row.transformationHash ?? undefined,
        transformationVersion: maybeVersion(row.transformationVersion),
        errorMessage: row.errorMessage ?? undefined,
        errorVersion: maybeVersion(row.errorVersion),
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

export class CVRStore {
  readonly #schema: string;
  readonly #taskID: string;
  readonly #id: string;
  readonly #failService: (e: unknown) => void;
  readonly #db: PostgresDB;
  readonly #upstreamDb: PostgresDB | undefined;
  readonly #writes: Set<{
    stats: Partial<CVRFlushStats>;
    write: (
      tx: PostgresTransaction,
      lastConnectTime: number,
    ) => PendingQuery<MaybeRow[]>;
  }> = new Set();
  readonly #upstreamWrites: ((
    tx: PostgresTransaction,
  ) => PendingQuery<MaybeRow[]>)[] = [];
  readonly #pendingRowRecordUpdates = new CustomKeyMap<RowID, RowRecord | null>(
    rowIDString,
  );
  readonly #forceUpdates = new CustomKeySet<RowID>(rowIDString);
  readonly #rowCache: RowRecordCache;
  readonly #loadAttemptIntervalMs: number;
  readonly #maxLoadAttempts: number;
  readonly #upstreamSchemaName: string;
  #rowCount: number = 0;

  constructor(
    lc: LogContext,
    cvrDb: PostgresDB,
    // Optionally undefined to deal with custom upstreams.
    // This is temporary until we have a more principled protocol to deal with
    // custom upstreams and clearing their custom mutator responses.
    // An implementor could simply clear them after N minutes for the time being.
    upstreamDb: PostgresDB | undefined,
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
    this.#upstreamDb = upstreamDb;
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
    this.#upstreamSchemaName = upstreamSchema(shard);
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
      assert(err);
      throw new ProtocolErrorWithLevel({
        kind: ErrorKind.ClientNotFound,
        message: `max attempts exceeded waiting for CVR@${err.cvrVersion} to catch up from ${err.rowsVersion}`,
        origin: ErrorOrigin.ZeroCache,
      });
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
    };

    const [instance, clientsRows, queryRows, desiresRows] =
      await this.#db.begin(Mode.READONLY, tx => [
        tx<
          (Omit<InstancesRow, 'clientGroupID'> & {rowsVersion: string | null})[]
        >`SELECT cvr."version", 
                 "lastActive",
                 "ttlClock",
                 "replicaVersion", 
                 "owner", 
                 "grantedAt",
                 "clientSchema", 
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
          "inactivatedAtMs" AS "inactivatedAt",
          "retryErrorVersion"
          FROM ${this.#cvr('desires')}
          WHERE "clientGroupID" = ${id}`,
      ]);

    if (instance.length === 0) {
      // This is the first time we see this CVR.
      this.putInstance({
        version: cvr.version,
        lastActive: 0,
        ttlClock: ttlClockFromNumber(0), // TTL clock starts at 0 for new instances
        replicaVersion: null,
        clientSchema: null,
      });
    } else {
      assert(instance.length === 1);
      const {
        version,
        lastActive,
        ttlClock,
        replicaVersion,
        owner,
        grantedAt,
        rowsVersion,
        clientSchema,
      } = instance[0];

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
      if (client) {
        if (!row.deleted && row.inactivatedAt === null) {
          client.desiredQueryIDs.push(row.queryHash);
        }
      } else {
        // This can happen if the client was deleted but the queries are still alive.
        lc.debug?.(`Client ${row.clientID} not found`);
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
          retryErrorVersion:
            row.retryErrorVersion === null
              ? undefined
              : versionFromString(row.retryErrorVersion),
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
    assert(result.length === 1);
    return result[0][0];
  }

  putInstance({
    version,
    replicaVersion,
    lastActive,
    clientSchema,
    ttlClock,
  }: Pick<
    CVRSnapshot,
    'version' | 'replicaVersion' | 'lastActive' | 'clientSchema' | 'ttlClock'
  >): void {
    this.#writes.add({
      stats: {instances: 1},
      write: (tx, lastConnectTime) => {
        const change: InstancesRow = {
          clientGroupID: this.#id,
          version: versionString(version),
          lastActive,
          ttlClock,
          replicaVersion,
          owner: this.#taskID,
          grantedAt: lastConnectTime,
          clientSchema,
        };
        return tx`
        INSERT INTO ${this.#cvr('instances')} ${tx(change)} 
          ON CONFLICT ("clientGroupID") DO UPDATE SET ${tx(change)}`;
      },
    });
  }

  markQueryAsDeleted(version: CVRVersion, queryPatch: QueryPatch): void {
    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`UPDATE ${this.#cvr('queries')} SET ${tx({
        patchVersion: versionString(version),
        deleted: true,
        transformationHash: null,
        transformationVersion: null,
        errorMessage: null,
        errorVersion: null,
      })}
      WHERE "clientGroupID" = ${this.#id} AND "queryHash" = ${queryPatch.id}`,
    });
  }

  putQuery(query: QueryRecord): void {
    const change: QueriesRow = queryRecordToQueryRow(this.#id, query);
    // ${JSON.stringify(change.queryArgs)}::text::json is used because postgres.js
    // gets confused if the input is `[boolean]` and throws an error saying a bool
    // cannot be converted to json.
    // https://github.com/porsager/postgres/issues/386
    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`INSERT INTO ${this.#cvr('queries')} (
        "clientGroupID",
        "queryHash",
        "clientAST",
        "queryName",
        "queryArgs",
        "patchVersion",
        "transformationHash",
        "transformationVersion",
        "internal",
        "deleted",
        "errorMessage",
        "errorVersion"
      ) VALUES (
        ${change.clientGroupID},
        ${change.queryHash},
        ${change.clientAST},
        ${change.queryName},
        ${change.queryArgs === undefined ? null : JSON.stringify(change.queryArgs)}::text::json,
        ${change.patchVersion},
        ${change.transformationHash ?? null},
        ${change.transformationVersion ?? null},
        ${change.internal},
        ${change.deleted ?? false},
        ${change.errorMessage ?? null},
        ${change.errorVersion ?? null}
      )
      ON CONFLICT ("clientGroupID", "queryHash")
      DO UPDATE SET 
        "clientAST" = ${change.clientAST},
        "queryName" = ${change.queryName},
        "queryArgs" = ${change.queryArgs === undefined ? null : JSON.stringify(change.queryArgs)}::text::json,
        "patchVersion" = ${change.patchVersion},
        "transformationHash" = ${change.transformationHash ?? null},
        "transformationVersion" = ${change.transformationVersion ?? null},
        "internal" = ${change.internal},
        "deleted" = ${change.deleted ?? false},
        "errorMessage" = ${change.errorMessage ?? null},
        "errorVersion" = ${change.errorVersion ?? null}`,
    });
  }

  updateQuery(query: QueryRecord) {
    const maybeVersionString = (v: CVRVersion | undefined) =>
      v ? versionString(v) : null;

    const change: Pick<
      QueriesRow,
      | 'patchVersion'
      | 'transformationHash'
      | 'transformationVersion'
      | 'deleted'
      | 'errorMessage'
      | 'errorVersion'
    > = {
      patchVersion:
        query.type === 'internal'
          ? null
          : maybeVersionString(query.patchVersion),
      transformationHash: query.transformationHash ?? null,
      transformationVersion: maybeVersionString(query.transformationVersion),
      deleted: false,
      errorMessage: query.errorMessage ?? null,
      errorVersion: maybeVersionString(query.errorVersion),
    };

    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`UPDATE ${this.#cvr('queries')} SET ${tx(change)}
      WHERE "clientGroupID" = ${this.#id} AND "queryHash" = ${query.id}`,
    });
  }

  insertClient(client: ClientRecord): void {
    const change: ClientsRow = {
      clientGroupID: this.#id,
      clientID: client.id,
    };

    this.#writes.add({
      stats: {clients: 1},
      write: tx => tx`INSERT INTO ${this.#cvr('clients')} ${tx(change)}`,
    });
  }

  deleteClient(clientID: string) {
    this.#writes.add({
      stats: {clients: 1},
      write: sql =>
        sql`DELETE FROM ${this.#cvr('clients')} 
            WHERE "clientGroupID" = ${this.#id} 
              AND "clientID" = ${clientID}`,
    });
    this.#upstreamWrites.push(
      sql =>
        sql`DELETE FROM ${sql(this.#upstreamSchemaName)}."mutations" 
            WHERE "clientGroupID" = ${this.#id} 
              AND "clientID" = ${clientID}`,
    );
  }

  putDesiredQuery(
    newVersion: CVRVersion,
    query: {id: string},
    client: {id: string},
    deleted: boolean,
    inactivatedAt: TTLClock | undefined,
    ttl: number,
    retryErrorVersion?: CVRVersion | undefined,
  ): void {
    const change: DesiresRow = {
      clientGroupID: this.#id,
      clientID: client.id,
      deleted,
      inactivatedAt: inactivatedAt ?? null,
      patchVersion: versionString(newVersion),
      queryHash: query.id,

      // ttl is in ms in JavaScript
      ttl: ttl < 0 ? null : ttl,
      retryErrorVersion: retryErrorVersion
        ? versionString(retryErrorVersion)
        : null,
    };

    // For backward compatibility during rollout, write to both old and new columns:
    // Old columns: inactivatedAt (TIMESTAMPTZ), ttl (INTERVAL) - need conversion ms->seconds
    // New columns: inactivatedAtMs (DOUBLE PRECISION), ttlMs (DOUBLE PRECISION) - store ms directly (1:1 with JS)
    const inactivatedAtTimestamp =
      inactivatedAt === undefined
        ? null
        : ttlClockFromNumber(ttlClockAsNumber(inactivatedAt) / 1000);
    const inactivatedAtMs = inactivatedAt ?? null;
    const ttlInterval = ttl < 0 ? null : ttl / 1000; // INTERVAL needs seconds
    const ttlMs = ttl < 0 ? null : ttl; // New column stores ms directly

    this.#writes.add({
      stats: {desires: 1},
      write: tx => tx`
      INSERT INTO ${this.#cvr('desires')} (
        "clientGroupID", "clientID", "queryHash", "patchVersion", "deleted",
        "ttl", "ttlMs", "inactivatedAt", "inactivatedAtMs", "retryErrorVersion"
      ) VALUES (
        ${change.clientGroupID}, ${change.clientID}, ${change.queryHash}, 
        ${change.patchVersion}, ${change.deleted}, ${ttlInterval}, ${ttlMs},
        ${inactivatedAtTimestamp}, ${inactivatedAtMs}, ${change.retryErrorVersion}
      )
      ON CONFLICT ("clientGroupID", "clientID", "queryHash")
      DO UPDATE SET
        "patchVersion" = ${change.patchVersion},
        "deleted" = ${change.deleted},
        "ttl" = ${ttlInterval},
        "ttlMs" = ${ttlMs},
        "inactivatedAt" = ${inactivatedAtTimestamp},
        "inactivatedAtMs" = ${inactivatedAtMs},
        "retryErrorVersion" = ${change.retryErrorVersion}
      `,
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
        assert(v);
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
    tx: PostgresTransaction,
    expectedCurrentVersion: CVRVersion,
    lastConnectTime: number,
  ): Promise<void> {
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
    if (owner !== this.#taskID && (grantedAt ?? 0) > lastConnectTime) {
      throw new OwnershipError(owner, grantedAt, lastConnectTime);
    }
    if (version !== expected) {
      throw new ConcurrentModificationException(expected, version);
    }
  }

  async #flush(
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
    if (this.#pendingRowRecordUpdates.size === 0 && this.#writes.size === 0) {
      return null;
    }
    // Note: The CVR instance itself is only updated if there are material
    // changes (i.e. changes to the CVR contents) to flush.
    this.putInstance(cvr);

    const rowsFlushed = await this.#db.begin(Mode.READ_COMMITTED, async tx => {
      // #checkVersionAndOwnership() executes a `SELECT ... FOR UPDATE`
      // query to acquire a row-level lock so that version-updating
      // transactions are effectively serialized per cvr.instance.
      //
      // Note that `rowsVersion` updates, on the other hand, are not subject
      // to this lock and can thus commit / be-committed independently of
      // cvr.instances.
      await this.#checkVersionAndOwnership(
        tx,
        expectedCurrentVersion,
        lastConnectTime,
      );

      const pipelined: Promise<unknown>[] = [];

      for (const write of this.#writes) {
        stats.instances += write.stats.instances ?? 0;
        stats.queries += write.stats.queries ?? 0;
        stats.desires += write.stats.desires ?? 0;
        stats.clients += write.stats.clients ?? 0;
        stats.rows += write.stats.rows ?? 0;

        pipelined.push(write.write(tx, lastConnectTime).execute());
        stats.statements++;
      }

      const rowUpdates = this.#rowCache.executeRowUpdates(
        tx,
        cvr.version,
        this.#pendingRowRecordUpdates,
        'allow-defer',
      );
      pipelined.push(...rowUpdates);
      stats.statements += rowUpdates.length;

      // Make sure Errors thrown by pipelined statements
      // are propagated up the stack.
      await Promise.all(pipelined);

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

    if (this.#upstreamDb) {
      await this.#upstreamDb.begin(Mode.READ_COMMITTED, async tx => {
        await Promise.all(this.#upstreamWrites.map(write => write(tx)));
      });
    }

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
    try {
      const stats = await this.#flush(
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
      this.#writes.clear();
      this.#upstreamWrites.length = 0;
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
