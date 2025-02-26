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
import {must} from '../../../../shared/src/must.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {astSchema} from '../../../../zero-protocol/src/ast.ts';
import * as ErrorKind from '../../../../zero-protocol/src/error-kind-enum.ts';
import * as Mode from '../../db/mode-enum.ts';
import {TransactionPool} from '../../db/transaction-pool.ts';
import {ErrorForClient, ErrorWithLevel} from '../../types/error-for-client.ts';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.ts';
import {rowIDString} from '../../types/row-key.ts';
import type {Patch, PatchToVersion} from './client-handler.ts';
import type {CVR, CVRSnapshot} from './cvr.ts';
import {RowRecordCache} from './row-record-cache.ts';
import {
  type ClientsRow,
  cvrSchema,
  type DesiresRow,
  type InstancesRow,
  type QueriesRow,
  type RowsRow,
} from './schema/cvr.ts';
import {
  type ClientQueryRecord,
  type ClientRecord,
  cmpVersions,
  type CVRVersion,
  EMPTY_CVR_VERSION,
  type InternalQueryRecord,
  type NullableCVRVersion,
  type QueryPatch,
  type QueryRecord,
  type RowID,
  type RowRecord,
  versionFromString,
  versionString,
} from './schema/types.ts';

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
  const ast = astSchema.parse(row.clientAST);
  const maybeVersion = (s: string | null) =>
    s === null ? undefined : versionFromString(s);
  return row.internal
    ? ({
        id: row.queryHash,
        ast,
        transformationHash: row.transformationHash ?? undefined,
        transformationVersion: maybeVersion(row.transformationVersion),
        internal: true,
      } satisfies InternalQueryRecord)
    : ({
        id: row.queryHash,
        ast,
        patchVersion: maybeVersion(row.patchVersion),
        desiredBy: {},
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

export class CVRStore {
  readonly #schema: string;
  readonly #taskID: string;
  readonly #id: string;
  readonly #db: PostgresDB;
  readonly #writes: Set<{
    stats: Partial<CVRFlushStats>;
    write: (
      tx: PostgresTransaction,
      lastConnectTime: number,
    ) => PendingQuery<MaybeRow[]>;
  }> = new Set();
  readonly #pendingRowRecordUpdates = new CustomKeyMap<RowID, RowRecord | null>(
    rowIDString,
  );
  readonly #forceUpdates = new CustomKeySet<RowID>(rowIDString);
  readonly #rowCache: RowRecordCache;
  readonly #loadAttemptIntervalMs: number;
  readonly #maxLoadAttempts: number;

  constructor(
    lc: LogContext,
    db: PostgresDB,
    shardID: string,
    taskID: string,
    cvrID: string,
    failService: (e: unknown) => void,
    loadAttemptIntervalMs = LOAD_ATTEMPT_INTERVAL_MS,
    maxLoadAttempts = MAX_LOAD_ATTEMPTS,
    deferredRowFlushThreshold = 100, // somewhat arbitrary
    setTimeoutFn = setTimeout,
  ) {
    this.#db = db;
    this.#schema = cvrSchema(shardID);
    this.#taskID = taskID;
    this.#id = cvrID;
    this.#rowCache = new RowRecordCache(
      lc,
      db,
      shardID,
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
      assert(err);
      throw new ErrorForClient({
        kind: ErrorKind.ClientNotFound,
        message: `max attempts exceeded waiting for CVR@${err.cvrVersion} to catch up from ${err.rowsVersion}`,
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
      replicaVersion: null,
      clients: {},
      queries: {},
    };

    const [instance, clientsRows, queryRows, desiresRows] =
      await this.#db.begin(tx => [
        tx<
          (Omit<InstancesRow, 'clientGroupID'> & {rowsVersion: string | null})[]
        >`SELECT cvr."version", 
                 "lastActive", 
                 "replicaVersion", 
                 "owner", 
                 "grantedAt", 
                 rows."version" as "rowsVersion"
            FROM ${this.#cvr('instances')} AS cvr
            LEFT JOIN ${this.#cvr('rowsVersion')} AS rows 
            ON cvr."clientGroupID" = rows."clientGroupID"
            WHERE cvr."clientGroupID" = ${id}`,
        tx<Pick<ClientsRow, 'clientID'>[]>`
        SELECT "clientID" FROM ${this.#cvr('clients')}
           WHERE "clientGroupID" = ${id}`,
        tx<QueriesRow[]>`
        SELECT * FROM ${this.#cvr('queries')} 
          WHERE "clientGroupID" = ${id} AND (deleted IS NULL OR deleted = FALSE)`,
        tx<DesiresRow[]>`SELECT 
          "clientGroupID",
          "clientID",
          "queryHash",
          "patchVersion",
          "deleted",
          EXTRACT(EPOCH FROM "ttl") * 1000 AS "ttl",
          "inactivatedAt"
          FROM ${this.#cvr('desires')}
          WHERE "clientGroupID" = ${id} AND (deleted IS NULL OR deleted = FALSE)`,
      ]);

    if (instance.length === 0) {
      // This is the first time we see this CVR.
      this.putInstance({
        version: cvr.version,
        lastActive: 0,
        replicaVersion: null,
      });
    } else {
      assert(instance.length === 1);
      const {
        version,
        lastActive,
        replicaVersion,
        owner,
        grantedAt,
        rowsVersion,
      } = instance[0];

      if (owner !== this.#taskID) {
        if ((grantedAt ?? 0) > lastConnectTime) {
          throw new OwnershipError(owner, grantedAt);
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
        `.execute();
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
      cvr.replicaVersion = replicaVersion;
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
      if (!client) {
        // should be impossible unless CVR is corrupted
        lc.error?.(`Client ${row.clientID} not found`, cvr);
        throw new Error(`Client ${row.clientID} not found`);
      }
      client.desiredQueryIDs.push(row.queryHash);

      const query = cvr.queries[row.queryHash];
      if (query && !query.internal) {
        query.desiredBy[row.clientID] = {
          inactivatedAt: row.inactivatedAt ?? undefined,
          ttl: row.ttl ?? undefined,
          version: versionFromString(row.patchVersion),
        };
      }
    }
    lc.debug?.(
      `loaded cvr@${versionString(cvr.version)} (${Date.now() - start} ms)`,
    );

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
   * This `delRowRecord()` method, on the other hand, is only used when a
   * row record is being *replaced* by another RowRecord, which currently
   * only happens when the columns of the row key change.
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

  putInstance({
    version,
    replicaVersion,
    lastActive,
  }: Pick<CVRSnapshot, 'version' | 'replicaVersion' | 'lastActive'>): void {
    this.#writes.add({
      stats: {instances: 1},
      write: (tx, lastConnectTime) => {
        const change: InstancesRow = {
          clientGroupID: this.#id,
          version: versionString(version),
          lastActive,
          replicaVersion,
          owner: this.#taskID,
          grantedAt: lastConnectTime,
        };
        return tx`
        INSERT INTO ${this.#cvr('instances')} ${tx(change)} 
          ON CONFLICT ("clientGroupID") DO UPDATE SET ${tx(change)}`;
      },
    });
  }

  #setLastActive(lastActive: number) {
    this.#writes.add({
      stats: {instances: 1},
      write: tx => tx`
        UPDATE ${this.#cvr('instances')} SET ${tx({lastActive})}
          WHERE "clientGroupID" = ${this.#id}
        `,
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
      })}
      WHERE "clientGroupID" = ${this.#id} AND "queryHash" = ${queryPatch.id}`,
    });
  }

  putQuery(query: QueryRecord): void {
    const maybeVersionString = (v: CVRVersion | undefined) =>
      v ? versionString(v) : null;

    const change: QueriesRow = query.internal
      ? {
          clientGroupID: this.#id,
          queryHash: query.id,
          clientAST: query.ast,
          patchVersion: null,
          transformationHash: query.transformationHash ?? null,
          transformationVersion: maybeVersionString(
            query.transformationVersion,
          ),
          internal: true,
          deleted: false, // put vs del "got" query
        }
      : {
          clientGroupID: this.#id,
          queryHash: query.id,
          clientAST: query.ast,
          patchVersion: maybeVersionString(query.patchVersion),
          transformationHash: query.transformationHash ?? null,
          transformationVersion: maybeVersionString(
            query.transformationVersion,
          ),
          internal: null,
          deleted: false, // put vs del "got" query
        };
    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`INSERT INTO ${this.#cvr('queries')} ${tx(change)}
      ON CONFLICT ("clientGroupID", "queryHash")
      DO UPDATE SET ${tx(change)}`,
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
    > = {
      patchVersion: query.internal
        ? null
        : maybeVersionString(query.patchVersion),
      transformationHash: query.transformationHash ?? null,
      transformationVersion: maybeVersionString(query.transformationVersion),
      deleted: false,
    };

    this.#writes.add({
      stats: {queries: 1},
      write: tx => tx`UPDATE ${this.#cvr('queries')} SET ${tx(change)}
      WHERE "clientGroupID" = ${this.#id} AND "queryHash" = ${query.id}`,
    });
  }

  /**
   * @param patchVersion This is only needed to allow old view syncers to function.
   */
  insertClient(client: ClientRecord, patchVersion: CVRVersion): void {
    const change: ClientsRow = {
      clientGroupID: this.#id,
      clientID: client.id,

      // Written so that exist clients that read do not fail.
      patchVersion: versionString(patchVersion),
      deleted: false,
    };

    this.#writes.add({
      stats: {clients: 1},
      write: tx => tx`INSERT INTO ${this.#cvr('clients')} ${tx(change)}`,
    });
  }

  deleteClient(clientID: string) {
    for (const name of ['desires', 'clients'] as const) {
      this.#writes.add({
        stats: {[name]: 1},
        write: tx =>
          tx`DELETE FROM ${this.#cvr(name)} WHERE "clientID" = ${clientID}`,
      });
    }
  }

  deleteClientGroup(clientGroupID: string) {
    for (const name of [
      'desires',
      'clients',
      'queries',
      'instances',
      'rows',
      'rowsVersion',
    ] as const) {
      this.#writes.add({
        stats: {[name]: 1},
        write: tx =>
          tx`DELETE FROM ${this.#cvr(
            name,
          )} WHERE "clientGroupID" = ${clientGroupID}`,
      });
    }
  }

  putDesiredQuery(
    newVersion: CVRVersion,
    query: {id: string},
    client: {id: string},
    deleted: boolean,
    inactivatedAt: number | undefined,
    ttl: number | undefined,
  ): void {
    const change: DesiresRow = {
      clientGroupID: this.#id,
      clientID: client.id,
      deleted,
      inactivatedAt: inactivatedAt ?? null,
      patchVersion: versionString(newVersion),
      queryHash: query.id,
      ttl: ttl ?? null,
    };
    this.#writes.add({
      stats: {desires: 1},
      write: tx => tx`
      INSERT INTO ${this.#cvr('desires')} ${tx(change)}
        ON CONFLICT ("clientGroupID", "clientID", "queryHash")
        DO UPDATE SET ${tx(change)}
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

      const ast = (id: string) => must(upToCVR.queries[id]).ast;

      const patches: PatchToVersion[] = [];
      for (const row of queryRows) {
        const {queryHash: id} = row;
        const patch: Patch = row.deleted
          ? {type: 'query', op: 'del', id}
          : {type: 'query', op: 'put', id, ast: ast(id)};
        const v = row.patchVersion;
        assert(v);
        patches.push({patch, toVersion: versionFromString(v)});
      }
      for (const row of allDesires) {
        const {clientID, queryHash: id} = row;
        const patch: Patch = row.deleted
          ? {type: 'query', op: 'del', id, clientID}
          : {type: 'query', op: 'put', id, clientID, ast: ast(id)};
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
      throw new OwnershipError(owner, grantedAt);
    }
    if (version !== expected) {
      throw new ConcurrentModificationException(expected, version);
    }
  }

  async #flush(
    expectedCurrentVersion: CVRVersion,
    newVersion: CVRVersion,
    skipNoopFlushes: boolean,
    lastConnectTime: number,
    lastActive: number,
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
    if (
      skipNoopFlushes &&
      this.#pendingRowRecordUpdates.size === 0 &&
      this.#writes.size === 0
    ) {
      return null;
    }
    this.#setLastActive(lastActive);

    const rowsFlushed = await this.#db.begin(async tx => {
      const pipelined: Promise<unknown>[] = [
        // #checkVersionAndOwnership() executes a `SELECT ... FOR UPDATE`
        // query to acquire a row-level lock so that version-updating
        // transactions are effectively serialized per cvr.instance.
        //
        // Note that `rowsVersion` updates, on the other hand, are not subject
        // to this lock and can thus commit / be-committed independently of
        // cvr.instances.
        this.#checkVersionAndOwnership(
          tx,
          expectedCurrentVersion,
          lastConnectTime,
        ),
      ];

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
        newVersion,
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
    await this.#rowCache.apply(
      this.#pendingRowRecordUpdates,
      newVersion,
      rowsFlushed,
    );
    return stats;
  }

  async flush(
    expectedCurrentVersion: CVRVersion,
    newVersion: CVRVersion,
    skipNoopFlushes: boolean,
    lastConnectTime: number,
    lastActive: number,
  ): Promise<CVRFlushStats | null> {
    try {
      return await this.#flush(
        expectedCurrentVersion,
        newVersion,
        skipNoopFlushes,
        lastConnectTime,
        lastActive,
      );
    } catch (e) {
      // Clear cached state if an error (e.g. ConcurrentModificationException) is encountered.
      this.#rowCache.clear();
      throw e;
    } finally {
      this.#writes.clear();
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

export class ConcurrentModificationException extends ErrorWithLevel {
  readonly name = 'ConcurrentModificationException';

  constructor(expectedVersion: string, actualVersion: string) {
    super(
      `CVR has been concurrently modified. Expected ${expectedVersion}, got ${actualVersion}`,
      'warn',
    );
  }
}

export class OwnershipError extends ErrorForClient {
  readonly name = 'OwnershipError';

  constructor(owner: string | null, grantedAt: number | null) {
    super(
      {
        kind: ErrorKind.Rehome,
        message:
          `CVR ownership was transferred to ${owner} at ` +
          `${new Date(grantedAt ?? 0).toISOString()}`,
        maxBackoffMs: 0,
      },
      'info',
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
