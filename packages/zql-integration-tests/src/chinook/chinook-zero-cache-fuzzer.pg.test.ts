import {expect} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {BigIntJSON} from '../../../shared/src/bigint-json.ts';
import {h128} from '../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../shared/src/queue.ts';
import type {NormalizedZeroConfig} from '../../../zero-cache/src/config/normalize.ts';
import {InspectorDelegate} from '../../../zero-cache/src/server/inspector-delegate.ts';
import {initializePostgresChangeSource} from '../../../zero-cache/src/services/change-source/pg/change-source.ts';
import {initializeStreamer} from '../../../zero-cache/src/services/change-streamer/change-streamer-service.ts';
import type {
  ChangeStreamer,
  ChangeStreamerService,
  Downstream,
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
import {Snapshotter} from '../../../zero-cache/src/services/view-syncer/snapshotter.ts';
import {
  type SyncContext,
  ViewSyncerService,
} from '../../../zero-cache/src/services/view-syncer/view-syncer.ts';
import {
  getConnectionURI,
  test,
  type PgTest,
} from '../../../zero-cache/src/test/db.ts';
import {DbFile} from '../../../zero-cache/src/test/lite.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import type {Source} from '../../../zero-cache/src/types/streams.ts';
import type {Subscription} from '../../../zero-cache/src/types/subscription.ts';
import {
  getPragmaConfig,
  setupReplica,
} from '../../../zero-cache/src/workers/replicator.ts';
import {
  ANYONE_CAN_DO_ANYTHING,
  definePermissions,
} from '../../../zero-permissions/src/permissions.ts';
import {mapAST, normalizeAST} from '../../../zero-protocol/src/ast.ts';
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
  Schema as ZeroSchema,
  TableSchema,
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
  checkQueryCases,
  enumerate,
  l1QueryCases,
  mutationQueryCases,
  panicIfFailed,
  skeletonQueryCases,
  swarmQueryCases,
  tailQueryCases,
} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData, miniPgContent} from './fuzz/mini.ts';
import {pushForSkeleton, type Mutation} from './fuzz/push.ts';
import {label as skeletonLabel, lower, type Skeleton} from './fuzz/skeleton.ts';
import {builder, schema} from './schema.ts';

const lc = createSilentLogContext();

const APP_ID = 'zql_integration_zero_cache_fuzzer';
const SHARD_NUM = 0;
const TASK_ID = 'zql-integration-zero-cache-fuzzer';
const PROTOCOL_CLIENT_GROUP_ID = 'zql-integration-protocol-fuzzer-client-group';
const PROTOCOL_CLIENT_ID = 'zql-integration-protocol-fuzzer-client';
const PROTOCOL_WS_ID = 'zql-integration-protocol-fuzzer-ws';
const TIMEOUT_MS = 120_000;
const PROTOCOL_WAIT_TIMEOUT_MS = 20_000;
const SEED = 0x00c0ffee;
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

const shard = {
  appID: APP_ID,
  shardNum: SHARD_NUM,
  publications: [],
};

const streamerOptions = {
  backPressureLimitHeapProportion: 0.04,
  flowControlConsensusPaddingSeconds: 1,
  statementTimeoutMs: 20_000,
  changeLogBatchSize: 2000,
};

const data = new Data(miniData, pkOf);
const L0_QUERY_CASES = skeletonQueryCases(
  enumerate({depth: 1, related: 1, exists: 1}),
);
const L1_QUERY_CASES = l1QueryCases(data);
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
const WRITE_FUZZ_CASES = WRITE_FUZZ_SKELETONS.map(s => ({
  label: `write|${skeletonLabel(s)}`,
  mutations: pushForSkeleton(data, s, 1),
  query: lower(s),
}));
const WRITE_FUZZ_WRITE_COUNT = WRITE_FUZZ_CASES.reduce(
  (n, c) => n + c.mutations.length,
  0,
);
const ZERO_CACHE_QUERY_CASES = [
  ...L0_QUERY_CASES,
  ...L1_QUERY_CASES.cases,
  ...swarmQueryCases(data, SEED, 16, 4),
  ...mutationQueryCases(
    enumerate({depth: 2, related: 1, exists: 1}).slice(0, 100),
    SEED ^ 0x5eed,
  ),
  ...tailQueryCases(CostModel.fromData(miniData, 1_000_000), SEED, 150).cases,
];
const PROTOCOL_QUERY_CASES = [
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
const PROTOCOL_WRITE_FUZZ_CASES = WRITE_FUZZ_CASES.filter(c =>
  PROTOCOL_WRITE_FUZZ_LABELS.has(c.label),
);
const PROTOCOL_WRITE_FUZZ_WRITE_COUNT = PROTOCOL_WRITE_FUZZ_CASES.reduce(
  (n, c) => n + c.mutations.length,
  0,
);

type ChinookSchema = typeof schema;
type ProtocolQueryCase = {
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

function parseStringifiedSource(source: Source<string>): Source<Downstream> {
  return {
    cancel: err => source.cancel(err),
    async *[Symbol.asyncIterator]() {
      for await (const msg of source) {
        yield BigIntJSON.parse(msg) as Downstream;
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

async function startZeroCacheReplica(testDBs: PgTest['testDBs']) {
  const cleanup: (() => Promise<void> | void)[] = [];

  try {
    const upstream = await testDBs.create(
      'chinook_zero_cache_fuzzer_upstream',
      {typeOpts: false},
    );
    const changeDB = await testDBs.create('chinook_zero_cache_fuzzer_change', {
      typeOpts: {sendStringAsJson: true},
    });
    cleanup.push(() => testDBs.drop(upstream, changeDB));

    await upstream.unsafe(miniPgContent());
    await upstream`CREATE SCHEMA ${upstream(APP_ID)}`;
    await upstream`
      CREATE TABLE ${upstream(APP_ID)}.permissions (
        permissions JSONB,
        hash TEXT,
        lock BOOL PRIMARY KEY DEFAULT true CHECK (lock)
      )`;
    await upstream`
      INSERT INTO ${upstream(APP_ID)}.permissions (permissions, hash)
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
      {level: 'error', format: 'text'},
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

    async function startProtocolClient() {
      const cvrDB = await testDBs.create('chinook_zero_cache_fuzzer_cvr');
      cleanup.push(() => testDBs.drop(cvrDB));
      await initViewSyncerSchema(lc, cvrDB, shard);

      const storageDB = new Database(lc, ':memory:');
      storageDB.prepare(CREATE_STORAGE_TABLE).run();
      cleanup.push(() => storageDB.close());

      const databaseStorage = new DatabaseStorage(storageDB);
      const operatorStorage = databaseStorage.createClientGroupStorage(
        PROTOCOL_CLIENT_GROUP_ID,
      );
      const config = {
        auth: {},
        query: {url: []},
        adminPassword: 'test-pwd',
        app: {id: APP_ID},
        replica: {file: replicaDbFile.path},
        log: {level: 'error'},
      } as unknown as NormalizedZeroConfig;
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
      const viewSyncer = new ViewSyncerService(
        config,
        lc,
        shard,
        TASK_ID,
        PROTOCOL_CLIENT_GROUP_ID,
        cvrDB,
        new PipelineDriver(
          lc.withContext('component', 'pipeline-driver'),
          testLogConfig,
          new Snapshotter(lc, replicaDbFile.path, shard),
          shard,
          operatorStorage,
          PROTOCOL_CLIENT_GROUP_ID,
          inspectorDelegate,
          () => 200,
        ),
        replicator.subscribe() as Subscription<ReplicaState>,
        new DrainCoordinator(),
        100,
        inspectorDelegate,
        connContextManager,
        undefined,
        (_lc, _description, op) => op(),
      );
      const viewSyncerDone = viewSyncer.run();
      cleanup.push(async () => {
        await viewSyncer.stop();
        await viewSyncerDone;
      });

      return ProtocolFuzzerClient.connect(viewSyncer);
    }

    return {
      upstream,
      pg,
      sqlite,
      startProtocolClient,
      async waitForReplicaVersion(description: string): Promise<ReplicaState> {
        const {done, value} = await withTimeout(
          versions.next(),
          `replica version after ${description}`,
        );
        if (done) {
          throw new Error(`replica notifications ended after ${description}`);
        }
        return value;
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

async function expectReplicaMatchesPG({
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

class ProtocolFuzzerClient {
  readonly #rows = new ProtocolRows();
  readonly #viewSyncer: ViewSyncerService;
  readonly #ctx: SyncContext;
  readonly #queue: Queue<ProtocolDownstream>;
  readonly #gotQueries = new Set<string>();

  private constructor(
    viewSyncer: ViewSyncerService,
    ctx: SyncContext,
    queue: Queue<ProtocolDownstream>,
  ) {
    this.#viewSyncer = viewSyncer;
    this.#ctx = ctx;
    this.#queue = queue;
  }

  static connect(viewSyncer: ViewSyncerService): ProtocolFuzzerClient {
    const ctx: SyncContext = {
      clientID: PROTOCOL_CLIENT_ID,
      profileID: 'p0000g00000000001',
      wsID: PROTOCOL_WS_ID,
      baseCookie: null,
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
        clientGroupID: PROTOCOL_CLIENT_GROUP_ID,
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
    viewSyncer.connContextManager.initConnection(selector, {
      desiredQueriesPatch: [],
      clientSchema: chinookClientSchema,
    });
    const source = viewSyncer.initConnection(selector, [
      'initConnection',
      {
        desiredQueriesPatch: [],
        clientSchema: chinookClientSchema,
      },
    ]);
    const queue = new Queue<ProtocolDownstream>();

    void (async () => {
      try {
        for await (const msg of source) {
          queue.enqueue(msg);
        }
      } catch (e) {
        queue.enqueueRejection(e);
      }
    })();

    return new ProtocolFuzzerClient(viewSyncer, ctx, queue);
  }

  async setQueries(cases: readonly ProtocolQueryCase[], label: string) {
    const puts = cases.map(c => putPatchFor(c.query));
    const putHashes = new Set(puts.map(p => p.hash));
    const expectDels = [...this.#gotQueries].filter(
      hash => !putHashes.has(hash),
    );
    for (const hash of expectDels) {
      this.#gotQueries.delete(hash);
    }
    const desiredQueriesPatch: UpQueriesPatch = [{op: 'clear'}, ...puts];
    await this.#changeDesiredQueries(
      desiredQueriesPatch,
      puts.map(p => p.hash),
      [],
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
    for (const {hash} of dels) {
      this.#gotQueries.delete(hash);
    }
    await this.#changeDesiredQueries(
      [...dels, ...puts],
      puts.map(p => p.hash),
      [],
      `protocol change queries ${label}`,
    );
  }

  async #changeDesiredQueries(
    desiredQueriesPatch: UpQueriesPatch,
    expectGotPuts: readonly string[],
    expectGotDels: readonly string[],
    description: string,
  ) {
    await this.#viewSyncer.changeDesiredQueries(
      {clientID: this.#ctx.clientID, wsID: this.#ctx.wsID},
      ['changeDesiredQueries', {desiredQueriesPatch}],
    );
    await this.#waitForGotQueryPatches(
      expectGotPuts,
      expectGotDels,
      description,
    );
  }

  async #waitForGotQueryPatches(
    expectPuts: readonly string[],
    expectDels: readonly string[],
    description: string,
  ) {
    if (this.#hasExpectedGotQueries(expectPuts, expectDels)) {
      return;
    }
    await this.#drainUntil(poke => {
      for (const msg of poke) {
        if (msg[0] !== 'pokePart') {
          continue;
        }
        for (const patch of msg[1].gotQueriesPatch ?? []) {
          if (patch.op === 'put') {
            this.#gotQueries.add(patch.hash);
          } else if (patch.op === 'del') {
            this.#gotQueries.delete(patch.hash);
          }
        }
      }
      return this.#hasExpectedGotQueries(expectPuts, expectDels);
    }, description);
  }

  #hasExpectedGotQueries(
    expectPuts: readonly string[],
    expectDels: readonly string[],
  ) {
    return (
      expectPuts.every(hash => this.#gotQueries.has(hash)) &&
      expectDels.every(hash => !this.#gotQueries.has(hash))
    );
  }

  async waitForCookieAtOrBeyond(
    stateVersion: string,
    description: string,
  ): Promise<void> {
    await this.#drainUntil(poke => {
      const cookie = pokeEndCookie(poke);
      return (
        cookie !== undefined &&
        cmpVersions(versionFromString(cookie), {stateVersion}) >= 0
      );
    }, `protocol poke for ${description}`);
  }

  run(query: AnyQuery) {
    return this.#rows.run(query);
  }

  async #drainUntil(
    done: (poke: readonly ProtocolDownstream[]) => boolean,
    description: string,
  ) {
    await withTimeout(
      (async () => {
        for (;;) {
          const poke = await this.#nextPoke();
          this.#rows.apply(poke);
          if (done(poke)) {
            return;
          }
        }
      })(),
      description,
      PROTOCOL_WAIT_TIMEOUT_MS,
    );
  }

  async #nextPoke(): Promise<ProtocolDownstream[]> {
    const poke: ProtocolDownstream[] = [];
    for (;;) {
      const msg = await this.#queue.dequeue();
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

function pokeEndCookie(
  poke: readonly ProtocolDownstream[],
): string | undefined {
  for (const msg of poke) {
    if (msg[0] === 'pokeEnd' && !msg[1].cancel) {
      return msg[1].cookie;
    }
  }
  return undefined;
}

function hashFor(query: AnyQuery): string {
  return hashOfAST(normalizeAST(asQueryInternals(query).ast));
}

function putPatchFor(query: AnyQuery) {
  const clientAST = normalizeAST(asQueryInternals(query).ast);
  return {
    op: 'put' as const,
    hash: hashOfAST(clientAST),
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

function mutationDescription(mutation: Mutation): string {
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

async function applyWriteFuzzMutation(
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

async function checkWriteFuzzCases(
  harness: Awaited<ReturnType<typeof startZeroCacheReplica>>,
): Promise<number> {
  let writeCount = 0;
  for (const c of WRITE_FUZZ_CASES) {
    await expectReplicaMatchesPG({...harness, query: c.query});
    for (let i = 0; i < c.mutations.length; i++) {
      const mutation = c.mutations[i];
      const description = `${c.label}#${i}:${mutationDescription(mutation)}`;
      await applyWriteFuzzMutation(harness.upstream, mutation);
      writeCount += 1;
      await harness.waitForReplicaVersion(description);
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

async function expectProtocolMatchesPG({
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

async function expectProtocolCasesMatchPG({
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

async function waitForProtocolAfterReplica(
  harness: Awaited<ReturnType<typeof startZeroCacheReplica>>,
  client: ProtocolFuzzerClient,
  description: string,
) {
  const state = await harness.waitForReplicaVersion(description);
  if (state.watermark === undefined) {
    throw new Error(`missing replica watermark after ${description}`);
  }
  await client.waitForCookieAtOrBeyond(state.watermark, description);
}

async function checkProtocolWriteFuzzCases(
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
      await applyWriteFuzzMutation(harness.upstream, mutation);
      writeCount += 1;
      const state = await harness.waitForReplicaVersion(description);
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

async function insertTrack(upstream: PostgresDB) {
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

async function moveTrackOutOfQuery(upstream: PostgresDB) {
  await upstream`
    UPDATE track
       SET album_id = 10
     WHERE track_id = 108`;
}

async function deleteTrack(upstream: PostgresDB) {
  await upstream`
    DELETE FROM track
     WHERE track_id = 105`;
}

async function deleteInsertedTrack(upstream: PostgresDB) {
  await upstream`
    DELETE FROM track
     WHERE track_id = 108`;
}

function albumTracksCase(id: number): ProtocolQueryCase {
  return {
    label: `album-${id}-tracks`,
    query: builder.album
      .where('id', '=', id)
      .related('tracks', t => t.orderBy('id', 'asc'))
      .one(),
  };
}

function tracksInAlbumCase(albumId: number): ProtocolQueryCase {
  return {
    label: `tracks-in-album-${albumId}`,
    query: builder.track
      .where('albumId', '=', albumId)
      .orderBy('id', 'asc')
      .related('album'),
  };
}

function trackByIDCase(id: number): ProtocolQueryCase {
  return {
    label: `track-${id}`,
    query: builder.track.where('id', '=', id).one(),
  };
}

function playlistTracksCase(id: number): ProtocolQueryCase {
  return {
    label: `playlist-${id}-tracks`,
    query: builder.playlist.where('id', '=', id).related('tracks').one(),
  };
}

test(
  `zero-cache replica stays query-equivalent to PostgreSQL for ${ZERO_CACHE_QUERY_CASES.length} generated query cases, ${WRITE_FUZZ_WRITE_COUNT} generated writes, and replicated writes`,
  {timeout: TIMEOUT_MS},
  async ({testDBs}: PgTest) => {
    const harness = await startZeroCacheReplica(testDBs);
    try {
      expect(L1_QUERY_CASES.coverage.fraction()).toBe(1);
      const report = await checkQueryCases(ZERO_CACHE_QUERY_CASES, query =>
        expectReplicaMatchesPG({...harness, query}),
      );
      expect(report.total).toBeGreaterThan(500);
      panicIfFailed(report, 12);

      expect(WRITE_FUZZ_CASES.length).toBeGreaterThan(10);
      const writeCount = await checkWriteFuzzCases(harness);
      expect(writeCount).toBe(WRITE_FUZZ_WRITE_COUNT);

      const query = builder.album
        .where('id', '=', 20)
        .related('tracks', t => t.orderBy('id', 'asc'))
        .one();

      await expectReplicaMatchesPG({...harness, query});

      await insertTrack(harness.upstream);
      await harness.waitForReplicaVersion('track insert');
      await expectReplicaMatchesPG({...harness, query});

      await moveTrackOutOfQuery(harness.upstream);
      await harness.waitForReplicaVersion('track update');
      await expectReplicaMatchesPG({...harness, query});

      await deleteTrack(harness.upstream);
      await harness.waitForReplicaVersion('track delete');
      await expectReplicaMatchesPG({...harness, query});
    } finally {
      await harness.cleanup();
    }
  },
);

test(
  `zero-cache protocol client stays query-equivalent to PostgreSQL for ${PROTOCOL_QUERY_CASES.length} generated query cases, ${PROTOCOL_WRITE_FUZZ_WRITE_COUNT} generated writes, and replicated writes`,
  {timeout: TIMEOUT_MS},
  async ({testDBs}: PgTest) => {
    const harness = await startZeroCacheReplica(testDBs);
    try {
      const client = await harness.startProtocolClient();
      await client.setQueries(
        [...PROTOCOL_QUERY_CASES, ...PROTOCOL_WRITE_FUZZ_CASES],
        'generated and write-fuzz query cases',
      );
      const report = await checkQueryCases(
        PROTOCOL_QUERY_CASES,
        async query => {
          await expectProtocolMatchesPG({...harness, client, query});
        },
      );
      expect(report.total).toBeGreaterThan(75);
      panicIfFailed(report, 8);

      expect(PROTOCOL_WRITE_FUZZ_CASES.length).toBeGreaterThan(5);
      const writeCount = await checkProtocolWriteFuzzCases(harness, client);
      expect(writeCount).toBe(PROTOCOL_WRITE_FUZZ_WRITE_COUNT);

      const query = builder.album
        .where('id', '=', 20)
        .related('tracks', t => t.orderBy('id', 'asc'))
        .one();

      await client.changeQueries({
        put: [{label: 'replicated writes', query}],
        label: 'replicated writes',
      });
      await expectProtocolMatchesPG({...harness, client, query});

      await insertTrack(harness.upstream);
      let state = await harness.waitForReplicaVersion('track insert');
      if (state.watermark === undefined) {
        throw new Error('missing replica watermark after track insert');
      }
      await client.waitForCookieAtOrBeyond(state.watermark, 'track insert');
      await expectProtocolMatchesPG({...harness, client, query});

      await moveTrackOutOfQuery(harness.upstream);
      state = await harness.waitForReplicaVersion('track update');
      if (state.watermark === undefined) {
        throw new Error('missing replica watermark after track update');
      }
      await client.waitForCookieAtOrBeyond(state.watermark, 'track update');
      await expectProtocolMatchesPG({...harness, client, query});

      await deleteTrack(harness.upstream);
      state = await harness.waitForReplicaVersion('track delete');
      if (state.watermark === undefined) {
        throw new Error('missing replica watermark after track delete');
      }
      await client.waitForCookieAtOrBeyond(state.watermark, 'track delete');
      await expectProtocolMatchesPG({...harness, client, query});
    } finally {
      await harness.cleanup();
    }
  },
);

test(
  'zero-cache protocol client maintains multiple queries through churn and batched writes',
  {timeout: TIMEOUT_MS},
  async ({testDBs}: PgTest) => {
    const harness = await startZeroCacheReplica(testDBs);
    try {
      const client = await harness.startProtocolClient();
      const album20 = albumTracksCase(20);
      const album10 = albumTracksCase(10);
      const tracks20 = tracksInAlbumCase(20);
      const track108 = trackByIDCase(108);
      const track105 = trackByIDCase(105);
      const playlist1 = playlistTracksCase(1);
      const baseline = [
        {label: 'all-tracks', query: builder.track},
        {label: 'all-playlist-tracks', query: builder.playlistTrack},
      ];

      let active = [album20, album10, tracks20, track108];
      await client.setQueries(
        [...baseline, ...active],
        'initial overlapping query set',
      );
      await expectProtocolCasesMatchPG({harness, client, cases: active});

      await insertTrack(harness.upstream);
      await moveTrackOutOfQuery(harness.upstream);
      await waitForProtocolAfterReplica(
        harness,
        client,
        'batched track insert',
      );
      await waitForProtocolAfterReplica(harness, client, 'batched track move');
      await expectProtocolCasesMatchPG({harness, client, cases: active});

      await client.changeQueries({
        del: [album20, tracks20],
        put: [track105, playlist1],
        label: 'remove album 20 queries and add track 105 plus playlist 1',
      });
      active = [album10, track108, track105, playlist1];
      await expectProtocolCasesMatchPG({harness, client, cases: active});

      await deleteInsertedTrack(harness.upstream);
      await deleteTrack(harness.upstream);
      await waitForProtocolAfterReplica(
        harness,
        client,
        'batched inserted track delete',
      );
      await waitForProtocolAfterReplica(
        harness,
        client,
        'batched existing track delete',
      );
      await expectProtocolCasesMatchPG({harness, client, cases: active});
    } finally {
      await harness.cleanup();
    }
  },
);
