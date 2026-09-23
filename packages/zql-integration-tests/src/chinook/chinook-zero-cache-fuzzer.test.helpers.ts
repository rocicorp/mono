/**
 * Harness for the zero-cache fuzzer: boots a replica (change-streamer, replicator
 * and view-syncer) over the mini chinook fixture and compares query results across
 * PostgreSQL, the replica and the protocol client. The tests live in the
 * `chinook-zero-cache-fuzzer*.pg.test.ts` files, split so CI can spread them over
 * test shards. The `.test.` in this file's name keeps it classified as test code
 * (excluded from CodeQL and from verify-package-deps) even though it defines no
 * tests itself.
 */
import {expect} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../shared/src/bigint-json.ts';
import {h128} from '../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import {Queue} from '../../../shared/src/queue.ts';
import type {NormalizedZeroConfig} from '../../../zero-cache/src/config/normalize.ts';
import {InspectorDelegate} from '../../../zero-cache/src/server/inspector-delegate.ts';
import {initializePostgresChangeSource} from '../../../zero-cache/src/services/change-source/pg/change-source-init.ts';
import {toStateVersionString} from '../../../zero-cache/src/services/change-source/pg/lsn.ts';
import {isPreSerializedBatch} from '../../../zero-cache/src/services/change-streamer/broadcast.ts';
import {
  initializeStreamer,
  type TuningOptions,
} from '../../../zero-cache/src/services/change-streamer/change-streamer-service.ts';
import type {
  ChangeStreamer,
  ChangeStreamerService,
  Downstream,
  SizedDownstream,
} from '../../../zero-cache/src/services/change-streamer/change-streamer.ts';
import {initChangeStreamerSchema} from '../../../zero-cache/src/services/change-streamer/schema/init.ts';
import {ReplicationStatusPublisher} from '../../../zero-cache/src/services/replicator/replication-status.ts';
import type {ReplicaState} from '../../../zero-cache/src/services/replicator/replicator.ts';
import {ReplicatorService} from '../../../zero-cache/src/services/replicator/replicator.ts';
import {ThreadWriteWorkerClient} from '../../../zero-cache/src/services/replicator/write-worker-client.ts';
import {ConnectionContextManagerImpl} from '../../../zero-cache/src/services/view-syncer/connection-context-manager.ts';
import {DrainCoordinator} from '../../../zero-cache/src/services/view-syncer/drain-coordinator.ts';
import {PipelineDriver} from '../../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import {initViewSyncerSchema} from '../../../zero-cache/src/services/view-syncer/schema/init.ts';
import {
  cmpVersions,
  versionFromString,
} from '../../../zero-cache/src/services/view-syncer/schema/types.ts';
import {SnapshotRowCache} from '../../../zero-cache/src/services/view-syncer/snapshot-row-cache.ts';
import {Snapshotter} from '../../../zero-cache/src/services/view-syncer/snapshotter.ts';
import {
  ViewSyncerService,
  type SyncContext,
} from '../../../zero-cache/src/services/view-syncer/view-syncer.ts';
import {
  getConnectionURI,
  type PgTest,
} from '../../../zero-cache/src/test/db.ts';
import {DbFile} from '../../../zero-cache/src/test/lite.ts';
import type {ViewSyncerDownstream} from '../../../zero-cache/src/types/downstream.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import type {
  PreSerialized,
  Source,
} from '../../../zero-cache/src/types/streams.ts';
import type {Subscription} from '../../../zero-cache/src/types/subscription.ts';
import {
  getPragmaConfig,
  setupReplica,
} from '../../../zero-cache/src/workers/replicator.ts';
import {
  ANYONE_CAN_DO_ANYTHING,
  definePermissions,
} from '../../../zero-permissions/src/permissions.ts';
import {
  type AST,
  type Condition,
  mapAST,
  normalizeAST,
} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Downstream as ProtocolDownstream} from '../../../zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../zero-protocol/src/queries-patch.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import type {RowPatchOp} from '../../../zero-protocol/src/row-patch.ts';
import {clientSchemaFrom} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  clientToServer,
  serverToClient,
} from '../../../zero-schema/src/name-mapper.ts';
import {getServerSchema} from '../../../zero-server/src/schema.ts';
import {Transaction} from '../../../zero-server/src/test/util.ts';
import type {
  TableSchema,
  Schema as ZeroSchema,
} from '../../../zero-types/src/schema.ts';
import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import {makeSourceChangeAdd} from '../../../zql/src/ivm/source.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../../zql/src/query/test/query-delegate.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../zqlite/src/database-storage.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../../zqlite/src/test/source-factory.ts';
import '../helpers/comparePg.ts';
import {TestPGQueryDelegate} from '../helpers/runner.ts';
import {pkOf} from './fuzz/axes.ts';
import {CostModel} from './fuzz/cost.ts';
import {
  enumerate,
  l1QueryCases,
  mutationQueryCases,
  skeletonQueryCases,
  swarmQueryCases,
  tailQueryCases,
} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData, miniPgContent} from './fuzz/mini.ts';
import {pushForQuery, type Mutation} from './fuzz/push.ts';
import {fuzzSeed} from './fuzz/seed.ts';
import {lower, label as skeletonLabel, type Skeleton} from './fuzz/skeleton.ts';
import {builder, schema} from './schema.ts';

const lc = createSilentLogContext();

const APP_ID = 'zql_integration_zero_cache_fuzzer';
const SHARD_NUM = 0;
const TASK_ID = 'zql-integration-zero-cache-fuzzer';
const PROTOCOL_CLIENT_GROUP_ID = 'zql-integration-protocol-fuzzer-client-group';
const PROTOCOL_CLIENT_ID = 'zql-integration-protocol-fuzzer-client';
export const TIMEOUT_MS = 120_000;
const PROTOCOL_WAIT_TIMEOUT_MS = 20_000;
/** The generator seed of the zero-cache fuzzer lanes (`ZERO_FUZZ_SEED`). */
const SEED = fuzzSeed();
export const FUZZ_SEED = SEED;
const writeSchema: ZeroSchema = schema;
const {clientSchema: chinookClientSchema} = clientSchemaFrom(schema);
const chinookClientToServer = clientToServer(schema.tables);
const maybeChinookPermissions = await definePermissions(schema, () => ({
  album: ANYONE_CAN_DO_ANYTHING,
  artist: ANYONE_CAN_DO_ANYTHING,
  customer: ANYONE_CAN_DO_ANYTHING,
  employee: ANYONE_CAN_DO_ANYTHING,
  genre: ANYONE_CAN_DO_ANYTHING,
  invoice: ANYONE_CAN_DO_ANYTHING,
  invoiceLine: ANYONE_CAN_DO_ANYTHING,
  mediaType: ANYONE_CAN_DO_ANYTHING,
  playlist: ANYONE_CAN_DO_ANYTHING,
  playlistTrack: ANYONE_CAN_DO_ANYTHING,
  track: ANYONE_CAN_DO_ANYTHING,
}));
if (!maybeChinookPermissions) {
  throw new Error('expected chinook permissions');
}
const chinookPermissions = maybeChinookPermissions;
const chinookPermissionsJSON = JSON.stringify(chinookPermissions);
const chinookPermissionsHash = h128(chinookPermissionsJSON).toString(16);

const streamerOptions: TuningOptions = {
  pgChangeLogEnabled: true,
  backPressureLimitHeapProportion: 0.04,
  flowControlConsensusTimeoutProportion: 2,
  statementTimeoutMs: 20_000,
  changeLogBatchSize: 2000,
};

const data = new Data(miniData, pkOf);
const L0_QUERY_CASES = skeletonQueryCases(
  enumerate({depth: 1, related: 1, exists: 1}),
);
export const L1_QUERY_CASES = l1QueryCases(data);
const WRITE_FUZZ_EXTRA_LABELS = [
  'album(ex:track)',
  'employee(ex:employee)',
  'invoice(ex:invoiceLine)',
  'playlist(ex:track)',
  'track(ex:playlist)',
];
const WRITE_FUZZ_SKELETONS = selectWriteFuzzSkeletons(
  enumerate({depth: 1, related: 1, exists: 1}),
);
export const WRITE_FUZZ_CASES = WRITE_FUZZ_SKELETONS.map(s => {
  const query = lower(s);
  return {
    label: `write|${skeletonLabel(s)}`,
    // Every table the query touches, including a junction's middle table.
    mutations: pushForQuery(data, s, asQueryInternals(query).ast, 1),
    query,
  };
});
export const WRITE_FUZZ_WRITE_COUNT = WRITE_FUZZ_CASES.reduce(
  (n, c) => n + c.mutations.length,
  0,
);
export const ZERO_CACHE_QUERY_CASES = [
  ...L0_QUERY_CASES,
  ...L1_QUERY_CASES.cases,
  ...swarmQueryCases(data, SEED, 16, 4),
  ...mutationQueryCases(
    enumerate({depth: 2, related: 1, exists: 1}).slice(0, 100),
    SEED ^ 0x5eed,
  ),
  ...tailQueryCases(CostModel.fromData(miniData, 1_000_000), SEED, 150).cases,
];
export const PROTOCOL_QUERY_CASES = [
  ...L0_QUERY_CASES,
  ...L1_QUERY_CASES.cases.slice(0, 30),
  ...swarmQueryCases(data, SEED ^ 0x7070, 4, 2),
  ...mutationQueryCases(
    enumerate({depth: 1, related: 1, exists: 1}).slice(0, 10),
    SEED ^ 0x7071,
  ),
  ...tailQueryCases(CostModel.fromData(miniData, 1_000_000), SEED ^ 0x7072, 10)
    .cases,
];
const PROTOCOL_WRITE_FUZZ_LABELS = new Set([
  'write|album(rel:artist)',
  'write|track(rel:album)',
  'write|playlist(rel:track)',
  'write|album(ex:track)',
  'write|employee(ex:employee)',
  'write|invoice(ex:invoiceLine)',
]);
export const PROTOCOL_WRITE_FUZZ_CASES = WRITE_FUZZ_CASES.filter(c =>
  PROTOCOL_WRITE_FUZZ_LABELS.has(c.label),
);
export const PROTOCOL_WRITE_FUZZ_WRITE_COUNT = PROTOCOL_WRITE_FUZZ_CASES.reduce(
  (n, c) => n + c.mutations.length,
  0,
);

type ChinookSchema = typeof schema;
export type ProtocolQueryCase = {
  readonly label: string;
  readonly query: AnyQuery;
};

function selectWriteFuzzSkeletons(
  skeletons: readonly Skeleton[],
): readonly Skeleton[] {
  const out: Skeleton[] = [];
  const seen = new Set<string>();

  const add = (s: Skeleton | undefined) => {
    if (!s) {
      return;
    }
    const key = skeletonLabel(s);
    if (!seen.has(key)) {
      seen.add(key);
      out.push(s);
    }
  };

  const roots = new Set(skeletons.map(s => s.table));
  for (const root of roots) {
    add(
      skeletons.find(
        s => s.table === root && s.children.some(c => c.kind === 'related'),
      ) ?? skeletons.find(s => s.table === root),
    );
  }
  for (const label of WRITE_FUZZ_EXTRA_LABELS) {
    add(skeletons.find(s => skeletonLabel(s) === label));
  }

  return out;
}

function parseStringifiedSource(
  source: Source<string | PreSerialized>,
): Source<SizedDownstream> {
  return {
    cancel: err => source.cancel(err),
    signal: source.signal,
    async *[Symbol.asyncIterator]() {
      for await (const item of source) {
        if (typeof item === 'string') {
          yield {data: BigIntJSON.parse(item) as Downstream, size: item.length};
        } else if (isPreSerializedBatch(item)) {
          for (const c of item.changes) {
            yield {
              data: BigIntJSON.parse(c[2]) as Downstream,
              size: c[2].length,
            };
          }
        }
      }
    },
  };
}

function parseStringifiedChangeStreamer(
  streamer: ChangeStreamerService,
): ChangeStreamer {
  return {
    async subscribe(ctx) {
      return parseStringifiedSource(await streamer.subscribe(ctx));
    },
  };
}

async function withTimeout<T>(
  promise: Promise<T>,
  description: string,
  timeoutMs = TIMEOUT_MS,
): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error(`timed out waiting for ${description}`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

/**
 * @param upstreamSetup SQL run on the upstream database after the mini
 *        fixture is loaded, before the replica is initialized.
 */
export async function startZeroCacheReplica(
  testDBs: PgTest['testDBs'],
  suite: string,
  upstreamSetup?: string | undefined,
) {
  // Replication slot names are derived from the app ID and are global to the
  // Postgres cluster, so each test file gets its own app ID.
  const appID = `${APP_ID}_${suite}`;
  const shard = {appID, shardNum: SHARD_NUM, publications: []};
  const cleanup: (() => Promise<void> | void)[] = [];

  try {
    const upstream = await testDBs.create(
      `chinook_zero_cache_fuzzer_${suite}_upstream`,
      {typeOpts: false},
    );
    const changeDB = await testDBs.create(
      `chinook_zero_cache_fuzzer_${suite}_change`,
      {
        typeOpts: {sendStringAsJson: true},
      },
    );
    cleanup.push(() => testDBs.drop(upstream, changeDB));

    await upstream.unsafe(miniPgContent());
    if (upstreamSetup) {
      await upstream.unsafe(upstreamSetup);
    }
    await upstream`CREATE SCHEMA ${upstream(appID)}`;
    await upstream`
      CREATE TABLE ${upstream(appID)}.permissions (
        permissions JSONB,
        hash TEXT,
        lock BOOL PRIMARY KEY DEFAULT true CHECK (lock)
      )`;
    await upstream`
      INSERT INTO ${upstream(appID)}.permissions (permissions, hash)
        VALUES (${chinookPermissions}, ${chinookPermissionsHash})`;

    const replicaDbFile = new DbFile('chinook-zero-cache-fuzzer');
    cleanup.push(() => replicaDbFile.delete());

    const {subscriptionState, changeSource} =
      await initializePostgresChangeSource(
        lc,
        getConnectionURI(upstream),
        shard,
        replicaDbFile.path,
        {tableCopyWorkers: 1},
        {suite: 'chinook-zero-cache-fuzzer'},
      );

    await setupReplica(lc, 'serving', {file: replicaDbFile.path});

    await initChangeStreamerSchema(lc, changeDB, shard);
    const changeStreamer = await initializeStreamer(
      lc,
      shard,
      TASK_ID,
      'change-streamer:zero-cache-fuzzer',
      'ws',
      changeDB,
      changeSource,
      ReplicationStatusPublisher.forReplicaFile(replicaDbFile.path, () =>
        Promise.resolve(),
      ),
      subscriptionState,
      null,
      null,
      true,
      streamerOptions,
    );
    const changeStreamerDone = changeStreamer.run();
    cleanup.push(async () => {
      await changeStreamer.stop();
      await changeStreamerDone;
    });

    const worker = new ThreadWriteWorkerClient();
    await worker.init(
      replicaDbFile.path,
      'serving',
      getPragmaConfig('serving'),
      {
        level: 'error',
        format: 'text',
      },
    );

    const replicator = new ReplicatorService(
      lc,
      TASK_ID,
      'chinook-zero-cache-fuzzer-replicator',
      'serving',
      parseStringifiedChangeStreamer(changeStreamer),
      worker,
      null,
    );
    const replicatorDone = replicator.run();
    cleanup.push(async () => {
      await replicator.stop();
      await replicatorDone;
    });

    const notifications = replicator.subscribe();
    const versions = notifications[Symbol.asyncIterator]();
    cleanup.push(() => notifications.cancel());
    await withTimeout(versions.next(), 'initial replica version');

    const replica = new Database(lc, replicaDbFile.path);
    cleanup.push(() => replica.close());
    const sqlite = newQueryDelegate(lc, testLogConfig, replica, schema);

    const serverSchema = await upstream.begin(tx =>
      getServerSchema(new Transaction(tx), schema),
    );
    const pg = new TestPGQueryDelegate(upstream, schema, serverSchema);

    let workers = 0;

    /**
     * Starts a sync worker. As in `server/syncer.ts`, the view-syncers of
     * the client groups on a worker share one CVR database and one operator
     * storage database. A `production` worker is also configured like a
     * production one: its view-syncers share a {@link SnapshotRowCache} and
     * plan their queries with the query planner (both on by default).
     */
    async function startSyncWorker({
      production,
    }: {
      production: boolean;
    }): Promise<SyncWorker> {
      const worker = workers++;
      const cvrDB = await testDBs.create(
        `chinook_zero_cache_fuzzer_${suite}_cvr${worker === 0 ? '' : worker}`,
      );
      cleanup.push(() => testDBs.drop(cvrDB));
      await initViewSyncerSchema(lc, cvrDB, shard);

      const storageDB = new Database(lc, ':memory:');
      storageDB.prepare(CREATE_STORAGE_TABLE).run();
      cleanup.push(() => storageDB.close());
      const databaseStorage = new DatabaseStorage(storageDB);

      const rowCache = production ? new SnapshotRowCache() : undefined;
      const config = {
        auth: {},
        query: {url: []},
        adminPassword: 'test-pwd',
        app: {id: appID},
        replica: {file: replicaDbFile.path},
        log: {level: 'error'},
        enableQueryPlanner: production,
      } as unknown as NormalizedZeroConfig;

      const newViewSyncer = (clientGroupID: string) => {
        const inspectorDelegate = new InspectorDelegate(undefined);
        const connContextManager = new ConnectionContextManagerImpl(
          lc,
          config.auth.revalidateIntervalSeconds,
          config.auth.retransformIntervalSeconds,
          {
            url: config.query.url,
            apiKey: config.query.apiKey,
            allowedClientHeaders: config.query.allowedClientHeaders,
            allowedRequestHeaders: config.query.allowedRequestHeaders,
            forwardCookies: config.query.forwardCookies,
          },
          {
            url: config.push?.url ?? config.mutate?.url,
            apiKey: config.push?.apiKey ?? config.mutate?.apiKey,
            allowedClientHeaders:
              config.push?.allowedClientHeaders ??
              config.mutate?.allowedClientHeaders,
            allowedRequestHeaders:
              config.push?.allowedRequestHeaders ??
              config.mutate?.allowedRequestHeaders,
            forwardCookies:
              config.push?.forwardCookies ??
              config.mutate?.forwardCookies ??
              false,
          },
        );
        return new ViewSyncerService(
          config,
          lc,
          shard,
          TASK_ID,
          clientGroupID,
          cvrDB,
          new PipelineDriver(
            lc.withContext('component', 'pipeline-driver'),
            testLogConfig,
            new Snapshotter(lc, replicaDbFile.path, shard, undefined, rowCache),
            shard,
            databaseStorage.createClientGroupStorage(clientGroupID),
            clientGroupID,
            inspectorDelegate,
            () => 200,
            production,
            production ? config : undefined,
          ),
          replicator.subscribe() as Subscription<ReplicaState>,
          new DrainCoordinator(),
          100,
          inspectorDelegate,
          connContextManager,
          undefined,
          (_lc, _description, op) => op(),
        );
      };

      return {
        startGroup: clientGroupID =>
          new SyncGroup(
            clientGroupID,
            () => newViewSyncer(clientGroupID),
            stop => cleanup.push(stop),
          ),
      };
    }

    async function startProtocolClient() {
      const worker = await startSyncWorker({production: false});
      const client = new ProtocolFuzzerClient(
        worker.startGroup(PROTOCOL_CLIENT_GROUP_ID),
        PROTOCOL_CLIENT_ID,
      );
      client.connect();
      return client;
    }

    return {
      upstream,
      pg,
      sqlite,
      startSyncWorker,
      startProtocolClient,
      async watermark(): Promise<string> {
        // A lower bound for "not yet caused by a write that hasn't
        // happened yet": the upstream LSN as of *before* issuing a write.
        // Any commit for that write is guaranteed to land at a later LSN,
        // so waiting for a notification >= this value can't be satisfied
        // by anything that already happened (e.g. shard bookkeeping, like
        // the replicas-table update that advances the slot's LSN on every
        // startStream()).
        const [{lsn}] = await upstream<{lsn: string}[]>`
          SELECT pg_current_wal_lsn() as lsn`;
        return toStateVersionString(lsn);
      },
      async waitForReplicaVersion(
        description: string,
        atOrBeyond = '',
      ): Promise<ReplicaState> {
        for (;;) {
          const {done, value} = await withTimeout(
            versions.next(),
            `replica version after ${description}`,
          );
          if (done) {
            throw new Error(`replica notifications ended after ${description}`);
          }
          if ((value.watermark ?? '') >= atOrBeyond) {
            return value;
          }
        }
      },
      async cleanup() {
        for (const fn of cleanup.reverse()) {
          await fn();
        }
        cleanup.length = 0;
      },
    };
  } catch (e) {
    for (const fn of cleanup.reverse()) {
      await fn();
    }
    throw e;
  }
}

export async function expectReplicaMatchesPG({
  pg,
  sqlite,
  query,
}: {
  pg: TestPGQueryDelegate;
  sqlite: ReturnType<typeof newQueryDelegate>;
  query: AnyQuery;
}) {
  const pgResult = await pg.run(query);
  const rootTable = asQueryInternals(query).ast
    .table as keyof ChinookSchema['tables'];
  const sqliteResult = mapResultToClientNames(
    await sqlite.run(query),
    schema,
    rootTable,
  );

  expect(sqliteResult).toEqualPg(pgResult);
}

class ProtocolRows {
  readonly #names = serverToClient(schema.tables);
  readonly #rows = new Map<string, Map<string, Row>>();

  apply(poke: readonly ProtocolDownstream[]) {
    for (const msg of poke) {
      if (msg[0] !== 'pokePart') {
        continue;
      }
      for (const patch of msg[1].rowsPatch ?? []) {
        this.#applyRowPatch(patch);
      }
    }
  }

  /** The rows of every table, in a canonical order (for comparing stores). */
  snapshot(): Record<string, Row[]> {
    const byKey = ([a]: [string, unknown], [b]: [string, unknown]) =>
      a < b ? -1 : a > b ? 1 : 0;
    return Object.fromEntries(
      [...this.#rows]
        .filter(([, rows]) => rows.size > 0)
        .sort(byKey)
        .map(([table, rows]) => [
          table,
          [...rows.entries()].toSorted(byKey).map(([, row]) => row),
        ]),
    );
  }

  run(query: AnyQuery) {
    const sources = Object.fromEntries(
      Object.entries(schema.tables).map(([table, tableSchema]) => {
        const src = new MemorySource(
          tableSchema.name,
          tableSchema.columns,
          tableSchema.primaryKey,
        );
        for (const row of this.#rows.get(table)?.values() ?? []) {
          consume(src.push(makeSourceChangeAdd(row)));
        }
        return [table, src];
      }),
    );
    const delegate = new TestMemoryQueryDelegate({sources});
    return delegate.run(query);
  }

  #applyRowPatch(patch: RowPatchOp) {
    if (patch.op === 'clear') {
      this.#rows.clear();
      return;
    }

    const table = this.#names.tableNameIfKnown(patch.tableName);
    if (!table) {
      return;
    }
    const rows = this.#tableRows(table);

    switch (patch.op) {
      case 'put': {
        const row = {...this.#names.row(patch.tableName, patch.value)} as Row;
        rows.set(primaryKeyKey(table, row), row);
        break;
      }
      case 'del': {
        const id = {...this.#names.row(patch.tableName, patch.id)} as Row;
        rows.delete(primaryKeyKey(table, id));
        break;
      }
      case 'update': {
        const id = {...this.#names.row(patch.tableName, patch.id)} as Row;
        const key = primaryKeyKey(table, id);
        const existing = rows.get(key);
        const merge =
          patch.merge === undefined
            ? undefined
            : ({...this.#names.row(patch.tableName, patch.merge)} as Row);
        const constrain = this.#names.columns(patch.tableName, patch.constrain);
        const next: Record<string, Row[string]> = {};
        const addConstrained = (row: Row) => {
          for (const [column, value] of Object.entries(row)) {
            if (!constrain?.length || constrain.includes(column)) {
              next[column] = value;
            }
          }
        };
        if (existing) {
          addConstrained(existing);
        }
        if (merge) {
          addConstrained(merge);
        }
        for (const column of tableSchema(table).primaryKey) {
          next[column] ??= id[column];
        }
        rows.set(key, next as Row);
        break;
      }
    }
  }

  #tableRows(table: string): Map<string, Row> {
    let rows = this.#rows.get(table);
    if (!rows) {
      rows = new Map();
      this.#rows.set(table, rows);
    }
    return rows;
  }
}

/**
 * A sync worker's client groups. See `startSyncWorker()` in
 * {@link startZeroCacheReplica}.
 */
export type SyncWorker = {
  startGroup(clientGroupID: string): SyncGroup;
};

/**
 * A client group on a {@link SyncWorker}. Its view-syncer can be stopped and
 * replaced, as the syncer's `ServiceRunner` replaces one that has shut down;
 * the replacement loads the client group's CVR from the CVR database.
 */
export class SyncGroup {
  readonly id: string;
  readonly #newViewSyncer: () => ViewSyncerService;
  readonly #onStart: (stop: () => Promise<void>) => void;
  #viewSyncer: ViewSyncerService | undefined;
  #done: Promise<void> = Promise.resolve();
  #starts = 0;

  constructor(
    id: string,
    newViewSyncer: () => ViewSyncerService,
    onStart: (stop: () => Promise<void>) => void,
  ) {
    this.id = id;
    this.#newViewSyncer = newViewSyncer;
    this.#onStart = onStart;
    this.start();
  }

  /** Starts a view-syncer for the group. */
  start(): void {
    assert(!this.#viewSyncer, `${this.id} is already running`);
    const viewSyncer = this.#newViewSyncer();
    const done = viewSyncer.run();
    this.#onStart(async () => {
      await viewSyncer.stop();
      await done;
    });
    this.#viewSyncer = viewSyncer;
    this.#done = done;
    this.#starts++;
  }

  /** How many view-syncers have served the group. */
  get starts(): number {
    return this.#starts;
  }

  /**
   * The view-syncer for a new connection. Like `ServiceRunner.getService()`,
   * this keeps the current one alive with `keepalive()`, or replaces it if it
   * is shutting down (i.e. its clients disconnected and its keepalive lapsed).
   */
  viewSyncerForConnection(): ViewSyncerService {
    const viewSyncer = must(this.#viewSyncer, `${this.id} is stopped`);
    if (!viewSyncer.keepalive()) {
      this.#viewSyncer = undefined;
      this.start();
    }
    return must(this.#viewSyncer);
  }

  /**
   * Keeps the view-syncer alive for its keepalive period after its last
   * client disconnects, as a connection does.
   */
  keepalive(): boolean {
    return must(this.#viewSyncer, `${this.id} is stopped`).keepalive();
  }

  async stop(): Promise<void> {
    const viewSyncer = must(this.#viewSyncer, `${this.id} is stopped`);
    this.#viewSyncer = undefined;
    await viewSyncer.stop();
    await this.#done;
  }
}

/**
 * Every query put by a {@link ProtocolFuzzerClient}, by hash, so that a client
 * group's queries can be re-desired from the hashes the server reports.
 */
const queriesByHash = new Map<string, AnyQuery>();

type ProtocolConnection = {
  readonly viewSyncer: ViewSyncerService;
  readonly selector: {readonly clientID: string; readonly wsID: string};
  readonly source: Source<ViewSyncerDownstream>;
  readonly queue: Queue<ProtocolDownstream>;
};

/**
 * A protocol client: it folds the pokes it receives into a row store, as
 * zero-client does. The store and cookie outlive a connection, so the
 * client can disconnect and reconnect with its cookie as the base cookie.
 */
export class ProtocolFuzzerClient {
  readonly group: SyncGroup;
  readonly clientID: string;
  readonly #rows = new ProtocolRows();
  readonly #gotQueries = new Set<string>();
  #cookie: string | null = null;
  #connections = 0;
  #conn: ProtocolConnection | undefined;

  constructor(group: SyncGroup, clientID: string) {
    this.group = group;
    this.clientID = clientID;
  }

  /** The cookie of the last poke applied to the store. */
  get cookie(): string | null {
    return this.#cookie;
  }

  get connected(): boolean {
    return this.#conn !== undefined;
  }

  /** The queries the client group has got, as reported by the server. */
  get gotQueries(): ReadonlySet<string> {
    return this.#gotQueries;
  }

  /**
   * Connects with the client's cookie as the base cookie (`null` the first
   * time), and optionally puts `queries` in the `initConnection` message, as
   * zero-client does for queries it added while disconnected.
   */
  connect(queries: readonly ProtocolQueryCase[] = []): void {
    assert(!this.#conn, `${this.clientID} is already connected`);
    const viewSyncer = this.group.viewSyncerForConnection();
    const ctx: SyncContext = {
      clientID: this.clientID,
      profileID: 'p0000g00000000001',
      wsID: `${this.clientID}-ws${this.#connections++}`,
      baseCookie: this.#cookie,
      protocolVersion: PROTOCOL_VERSION,
      httpCookie: undefined,
      origin: undefined,
      userID: 'user-1',
      auth: undefined,
    };
    const selector = {clientID: ctx.clientID, wsID: ctx.wsID};
    viewSyncer.connContextManager.registerConnection(
      selector,
      {
        protocolVersion: ctx.protocolVersion,
        clientID: ctx.clientID,
        clientGroupID: this.group.id,
        profileID: ctx.profileID,
        baseCookie: ctx.baseCookie,
        timestamp: Date.now(),
        lmID: 0,
        wsID: ctx.wsID,
        debugPerf: false,
        auth: undefined,
        userID: ctx.userID,
        initConnectionMsg: undefined,
        httpCookie: ctx.httpCookie,
        origin: ctx.origin,
      },
      ctx.auth,
    );
    const body = {
      desiredQueriesPatch: queries.map(c => putPatchFor(c.query)),
      // As in zero-client, the schema is only sent without a base cookie.
      ...(ctx.baseCookie === null ? {clientSchema: chinookClientSchema} : {}),
    };
    viewSyncer.connContextManager.initConnection(selector, body);
    const source = viewSyncer.initConnection(selector, [
      'initConnection',
      body,
    ]);
    const queue = new Queue<ProtocolDownstream>();

    void (async () => {
      try {
        for await (const {message} of source) {
          queue.enqueue(message);
        }
      } catch (e) {
        queue.enqueueRejection(e);
      }
    })();

    this.#conn = {viewSyncer, selector, source, queue};
  }

  /**
   * Closes the connection. Pokes that were received but not yet applied are
   * dropped, so the client reconnects from the last poke it applied.
   */
  disconnect(): void {
    const conn = must(this.#conn, `${this.clientID} is not connected`);
    this.#conn = undefined;
    conn.source.cancel();
  }

  async setQueries(cases: readonly ProtocolQueryCase[], label: string) {
    const puts = cases.map(c => putPatchFor(c.query));
    const desiredQueriesPatch: UpQueriesPatch = [{op: 'clear'}, ...puts];
    await this.#changeDesiredQueries(
      desiredQueriesPatch,
      puts.map(p => p.hash),
      `protocol set queries ${label}`,
    );
  }

  async changeQueries({
    put = [],
    del = [],
    label,
  }: {
    put?: readonly ProtocolQueryCase[] | undefined;
    del?: readonly ProtocolQueryCase[] | undefined;
    label: string;
  }) {
    const puts = put.map(c => putPatchFor(c.query));
    const dels = del.map(c => ({op: 'del' as const, hash: hashFor(c.query)}));
    await this.#changeDesiredQueries(
      [...dels, ...puts],
      puts.map(p => p.hash),
      `protocol change queries ${label}`,
    );
  }

  async #changeDesiredQueries(
    desiredQueriesPatch: UpQueriesPatch,
    expectGotPuts: readonly string[],
    description: string,
  ) {
    const conn = must(this.#conn, `${this.clientID} is not connected`);
    await conn.viewSyncer.changeDesiredQueries(conn.selector, [
      'changeDesiredQueries',
      {desiredQueriesPatch},
    ]);
    await this.waitForGotQueries(expectGotPuts, description);
  }

  /**
   * Waits until the server reports that the client group has got the
   * queries with the given hashes. (A deleted query is not awaited: it stays
   * got until its TTL expires.)
   */
  async waitForGotQueries(hashes: readonly string[], description: string) {
    const got = () => hashes.every(hash => this.#gotQueries.has(hash));
    if (!got()) {
      await this.#drainUntil(got, description);
    }
  }

  /**
   * Waits for a poke whose cookie is at or beyond the replica `stateVersion`.
   */
  async waitForCookieAtOrBeyond(
    stateVersion: string,
    description: string,
  ): Promise<void> {
    const reached = () =>
      this.#cookie !== null &&
      cmpVersions(versionFromString(this.#cookie), {stateVersion}) >= 0;
    if (!reached()) {
      await this.#drainUntil(reached, `protocol poke for ${description}`);
    }
  }

  /** Waits for a poke whose cookie is at or beyond `cookie`. */
  async waitForCookie(cookie: string, description: string): Promise<void> {
    const reached = () =>
      this.#cookie !== null &&
      cmpVersions(versionFromString(this.#cookie), versionFromString(cookie)) >=
        0;
    if (!reached()) {
      await this.#drainUntil(reached, `protocol poke for ${description}`);
    }
  }

  run(query: AnyQuery) {
    return this.#rows.run(query);
  }

  /** The rows in the client's store, in a canonical order. */
  rows(): Record<string, Row[]> {
    return this.#rows.snapshot();
  }

  async #drainUntil(done: () => boolean, description: string) {
    await withTimeout(
      (async () => {
        do {
          this.#apply(await this.#nextPoke());
        } while (!done());
      })(),
      `${this.clientID}: ${description}`,
      PROTOCOL_WAIT_TIMEOUT_MS,
    );
  }

  /**
   * Applies a poke to the store, or drops it if it was canceled. Like
   * Replicache, the client rejects a poke whose base cookie is not its
   * cookie: the server would be patching a store the client does not have.
   */
  #apply(poke: readonly ProtocolDownstream[]) {
    const start = poke[0];
    const end = must(poke.at(-1));
    assert(start[0] === 'pokeStart' && end[0] === 'pokeEnd', 'partial poke');
    if (end[1].cancel) {
      return;
    }
    if ((start[1].baseCookie ?? null) !== this.#cookie) {
      throw new Error(
        `${this.clientID}: unexpected base cookie ${start[1].baseCookie} ` +
          `for poke ${start[1].pokeID}, client is at ${this.#cookie}`,
      );
    }
    this.#rows.apply(poke);
    for (const msg of poke) {
      if (msg[0] !== 'pokePart') {
        continue;
      }
      for (const patch of msg[1].gotQueriesPatch ?? []) {
        switch (patch.op) {
          case 'put':
            this.#gotQueries.add(patch.hash);
            break;
          case 'del':
            this.#gotQueries.delete(patch.hash);
            break;
          case 'clear':
            this.#gotQueries.clear();
            break;
        }
      }
    }
    this.#cookie = end[1].cookie;
  }

  async #nextPoke(): Promise<ProtocolDownstream[]> {
    const {queue} = must(this.#conn, `${this.clientID} is not connected`);
    const poke: ProtocolDownstream[] = [];
    for (;;) {
      const msg = await queue.dequeue();
      switch (msg[0]) {
        case 'pokeStart':
        case 'pokePart':
        case 'pokeEnd':
          poke.push(msg);
          if (msg[0] === 'pokeEnd') {
            return poke;
          }
          break;
        case 'transformError':
        case 'error':
          throw new Error(`unexpected protocol message ${JSON.stringify(msg)}`);
      }
    }
  }
}

/** Looks up a query put by a {@link ProtocolFuzzerClient} by its hash. */
export function queryForHash(hash: string): AnyQuery {
  return must(queriesByHash.get(hash), `unknown query hash ${hash}`);
}

/**
 * The AST that zero-client would send for `query`. The fuzz generators build
 * queries with the permissions builder (`newStaticQuery`), which marks every
 * subquery as a `permissions` subquery, and zero-cache does not sync the rows
 * of those. zero-client marks them as `client` subqueries.
 */
function clientASTFor(query: AnyQuery): AST {
  return normalizeAST(asClientAST(asQueryInternals(query).ast));
}

function asClientAST(ast: AST): AST {
  return {
    ...ast,
    ...(ast.where ? {where: asClientCondition(ast.where)} : {}),
    ...(ast.related
      ? {
          related: ast.related.map(r => ({
            ...r,
            system: 'client' as const,
            subquery: asClientAST(r.subquery),
          })),
        }
      : {}),
  };
}

function asClientCondition(cond: Condition): Condition {
  switch (cond.type) {
    case 'and':
    case 'or':
      return {...cond, conditions: cond.conditions.map(asClientCondition)};
    case 'correlatedSubquery':
      return {
        ...cond,
        related: {
          ...cond.related,
          system: 'client',
          subquery: asClientAST(cond.related.subquery),
        },
      };
    default:
      return cond;
  }
}

export function hashFor(query: AnyQuery): string {
  return hashOfAST(clientASTFor(query));
}

function putPatchFor(query: AnyQuery) {
  const clientAST = clientASTFor(query);
  const hash = hashOfAST(clientAST);
  queriesByHash.set(hash, query);
  return {
    op: 'put' as const,
    hash,
    ast: mapAST(clientAST, chinookClientToServer),
  };
}

function primaryKeyKey(table: string, row: Row): string {
  return JSON.stringify(
    tableSchema(table).primaryKey.map(column =>
      definedRowValue(table, column, row),
    ),
  );
}

function tableSchema(table: string): TableSchema {
  const tableDef = writeSchema.tables[table];
  if (!tableDef) {
    throw new Error(`unknown table ${table}`);
  }
  return tableDef;
}

function serverTableName(table: string): string {
  const tableDef = tableSchema(table);
  return tableDef.serverName ?? table;
}

function serverColumnName(table: string, column: string): string {
  const columnDef = tableSchema(table).columns[column];
  if (!columnDef) {
    throw new Error(`unknown column ${table}.${column}`);
  }
  return columnDef.serverName ?? column;
}

function toServerRow(table: string, row: Row): Row {
  const out: Record<string, Row[string]> = {};
  for (const [column, value] of Object.entries(row)) {
    out[serverColumnName(table, column)] = value;
  }
  return out;
}

function definedRowValue(
  table: string,
  column: string,
  row: Row,
): Exclude<Row[string], undefined> {
  const value = row[column];
  if (value === undefined) {
    throw new Error(`missing value for ${table}.${column}`);
  }
  return value;
}

function primaryKeyConditions(upstream: PostgresDB, table: string, row: Row) {
  return tableSchema(table).primaryKey.flatMap((column, i) => {
    const condition = upstream`${upstream(serverColumnName(table, column))} = ${definedRowValue(
      table,
      column,
      row,
    )}`;
    return i === 0 ? [condition] : [upstream`AND`, condition];
  });
}

export function mutationDescription(mutation: Mutation): string {
  const pks = tableSchema(mutation.table)
    .primaryKey.map(
      column => `${column}=${JSON.stringify(mutation.row[column])}`,
    )
    .join(',');
  return `${mutation.kind} ${mutation.table}(${pks})`;
}

function expectSingleAffectedRow(
  result: readonly unknown[],
  description: string,
) {
  if (result.length !== 1) {
    throw new Error(
      `${description}: expected exactly one affected row, got ${result.length}`,
    );
  }
}

export async function applyWriteFuzzMutation(
  upstream: PostgresDB,
  mutation: Mutation,
) {
  const table = serverTableName(mutation.table);
  switch (mutation.kind) {
    case 'remove': {
      const result = await upstream`
        DELETE FROM ${upstream(table)}
         WHERE ${primaryKeyConditions(upstream, mutation.table, mutation.row)}
        RETURNING 1`;
      expectSingleAffectedRow(result, mutationDescription(mutation));
      break;
    }
    case 'add': {
      const result = await upstream`
        INSERT INTO ${upstream(table)}
        ${upstream(toServerRow(mutation.table, mutation.row))}
        RETURNING 1`;
      expectSingleAffectedRow(result, mutationDescription(mutation));
      break;
    }
    case 'edit': {
      const result = await upstream`
        UPDATE ${upstream(table)}
           SET ${upstream(toServerRow(mutation.table, mutation.row))}
         WHERE ${primaryKeyConditions(upstream, mutation.table, mutation.old)}
        RETURNING 1`;
      expectSingleAffectedRow(result, mutationDescription(mutation));
      break;
    }
  }
}

export async function checkWriteFuzzCases(
  harness: Awaited<ReturnType<typeof startZeroCacheReplica>>,
): Promise<number> {
  let writeCount = 0;
  for (const c of WRITE_FUZZ_CASES) {
    await expectReplicaMatchesPG({...harness, query: c.query});
    for (let i = 0; i < c.mutations.length; i++) {
      const mutation = c.mutations[i];
      const description = `${c.label}#${i}:${mutationDescription(mutation)}`;
      const baseline = await harness.watermark();
      await applyWriteFuzzMutation(harness.upstream, mutation);
      writeCount += 1;
      await harness.waitForReplicaVersion(description, baseline);
      try {
        await expectReplicaMatchesPG({...harness, query: c.query});
      } catch (e) {
        const msg = e instanceof Error ? (e.stack ?? e.message) : String(e);
        throw new Error(`write fuzz divergence after ${description}\n${msg}`);
      }
    }
  }
  return writeCount;
}

export async function expectProtocolMatchesPG({
  pg,
  client,
  query,
}: {
  pg: TestPGQueryDelegate;
  client: ProtocolFuzzerClient;
  query: AnyQuery;
}) {
  const pgResult = await pg.run(query);
  const protocolResult = await client.run(query);

  expect(protocolResult).toEqualPg(pgResult);
}

export async function expectProtocolCasesMatchPG({
  harness,
  client,
  cases,
}: {
  harness: Awaited<ReturnType<typeof startZeroCacheReplica>>;
  client: ProtocolFuzzerClient;
  cases: readonly ProtocolQueryCase[];
}) {
  for (const c of cases) {
    try {
      await expectProtocolMatchesPG({...harness, client, query: c.query});
    } catch (e) {
      const msg = e instanceof Error ? (e.stack ?? e.message) : String(e);
      throw new Error(`protocol divergence for ${c.label}\n${msg}`);
    }
  }
}

export async function waitForProtocolAfterReplica(
  harness: Awaited<ReturnType<typeof startZeroCacheReplica>>,
  client: ProtocolFuzzerClient,
  description: string,
  baseline?: string,
) {
  const state = await harness.waitForReplicaVersion(description, baseline);
  if (state.watermark === undefined) {
    throw new Error(`missing replica watermark after ${description}`);
  }
  await client.waitForCookieAtOrBeyond(state.watermark, description);
}

export async function checkProtocolWriteFuzzCases(
  harness: Awaited<ReturnType<typeof startZeroCacheReplica>>,
  client: ProtocolFuzzerClient,
): Promise<number> {
  await expectProtocolCasesMatchPG({
    harness,
    client,
    cases: PROTOCOL_WRITE_FUZZ_CASES,
  });

  let writeCount = 0;
  for (const c of PROTOCOL_WRITE_FUZZ_CASES) {
    for (let i = 0; i < c.mutations.length; i++) {
      const mutation = c.mutations[i];
      const description = `${c.label}#${i}:${mutationDescription(mutation)}`;
      const baseline = await harness.watermark();
      await applyWriteFuzzMutation(harness.upstream, mutation);
      writeCount += 1;
      const state = await harness.waitForReplicaVersion(description, baseline);
      if (state.watermark === undefined) {
        throw new Error(`missing replica watermark after ${description}`);
      }
      await client.waitForCookieAtOrBeyond(state.watermark, description);
      try {
        await expectProtocolCasesMatchPG({
          harness,
          client,
          cases: [c],
        });
      } catch (e) {
        const msg = e instanceof Error ? (e.stack ?? e.message) : String(e);
        throw new Error(
          `protocol write fuzz divergence after ${description}\n${msg}`,
        );
      }
    }
  }
  return writeCount;
}

export async function insertTrack(upstream: PostgresDB) {
  await upstream`
    INSERT INTO track (
      track_id,
      name,
      album_id,
      media_type_id,
      genre_id,
      composer,
      milliseconds,
      bytes,
      unit_price
    ) VALUES (
      108,
      't-replicated',
      20,
      1,
      2,
      'Zero',
      123000,
      123456,
      0.99
    )`;
}

export async function moveTrackOutOfQuery(upstream: PostgresDB) {
  await upstream`
    UPDATE track
       SET album_id = 10
     WHERE track_id = 108`;
}

export async function deleteTrack(upstream: PostgresDB) {
  await upstream`
    DELETE FROM track
     WHERE track_id = 105`;
}

export async function deleteInsertedTrack(upstream: PostgresDB) {
  await upstream`
    DELETE FROM track
     WHERE track_id = 108`;
}

export function albumTracksCase(id: number): ProtocolQueryCase {
  return {
    label: `album-${id}-tracks`,
    query: builder.album
      .where('id', '=', id)
      .related('tracks', t => t.orderBy('id', 'asc'))
      .one(),
  };
}

export function tracksInAlbumCase(albumId: number): ProtocolQueryCase {
  return {
    label: `tracks-in-album-${albumId}`,
    query: builder.track
      .where('albumId', '=', albumId)
      .orderBy('id', 'asc')
      .related('album'),
  };
}

export function trackByIDCase(id: number): ProtocolQueryCase {
  return {
    label: `track-${id}`,
    query: builder.track.where('id', '=', id).one(),
  };
}

export function playlistTracksCase(id: number): ProtocolQueryCase {
  return {
    label: `playlist-${id}-tracks`,
    query: builder.playlist.where('id', '=', id).related('tracks').one(),
  };
}
