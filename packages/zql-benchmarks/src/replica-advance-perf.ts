import {spawn} from 'node:child_process';
import {constants as fsConstants, rmSync} from 'node:fs';
import {copyFile, mkdtemp, readFile, rm, stat} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {build, type Plugin} from 'esbuild';

type Options = {
  replica: string;
  queriesDir: string;
  users: number;
  batchSize: number;
  queryPattern: RegExp | undefined;
  legacyCoverJoin: boolean;
  keepClone: boolean;
  json: boolean;
  workload: Workload;
  hotWorks: number;
  viewerPin: boolean;
  user: string | undefined;
};

type Workload = 'background' | 'cover' | 'homeview';

type Row = Readonly<Record<string, unknown>>;

type Runtime = {
  BACKGROUND_QUERIES: readonly {readonly name: string}[];
  backgroundQueriesRequests: (
    userID: string,
    now: number,
  ) => readonly {
    readonly args: unknown;
    readonly query: {readonly queryName: string};
  }[];
  queries: Record<string, QueryDefinition>;
  schema: unknown;
  Database: new (lc: unknown, file: string) => Database;
  newQueryDelegate: (
    lc: unknown,
    logConfig: unknown,
    db: Database,
    schema: unknown,
  ) => QueryDelegate;
  asQueryInternals: (query: unknown) => {readonly ast: unknown};
  createSilentLogContext: () => unknown;
  makeSourceChangeEdit: (row: Row, oldRow: Row) => unknown;
  testLogConfig: unknown;
};

type QueryDefinition = {
  fn: (input: {args: unknown; ctx: unknown}) => unknown;
};

type Database = {
  close(): void;
  exec(sql: string): void;
  prepare(sql: string): {
    all(...args: unknown[]): Row[];
  };
};

type View = {
  destroy(): void;
  flush(): void;
};

type Source = {
  push(change: unknown): Iterable<unknown>;
};

type QueryDelegate = {
  getSource(table: string): Source;
  materialize(query: unknown): View;
};

type Result = {
  userID: string;
  query: string;
  readsWorkCovers: boolean;
  hydrateMs: number;
  pushMs: number;
  flushMs: number;
  totalMs: number;
  error?: string | undefined;
};

type WorkloadRequest = {
  args: unknown;
  name: string;
};

const ZERO_SQLITE_IMPORT = /^@rocicorp\/zero-sqlite3$/;
const ZERO_IMPORT = /^@rocicorp\/zero$/;
const EFFECT_IMPORT = /^effect$/;
const ZERO_CONTEXT_IMPORT = /^@workspace\/zero-context$/;
const USER_ID_IMPORT = /^@workspace\/(authentication|identifiers)$/;
const EFFECT_SCHEMA_IMPORT = /^@workspace\/effect-schema$/;
const COMMERCE_CORE_IMPORT = /^@workspace\/commerce-core$/;
const ISBN_IMPORT = /^@workspace\/isbn$/;
const SOCIAL_CONNECTIONS_IMPORT = /^@workspace\/social-connections$/;
const ZERO_SCHEMA_IMPORT = /^@workspace\/(.+-zero-schema)$/;
const ALL_PATHS = /.*/;
const CATALOG_SCHEMA_PATH = /catalog\/zero-schema\/src\/schema\.ts$/;
const HOME_QUERIES_PATH = /home\/zero-schema\/src\/queries\.ts$/;

const HOME_VIEW_PASSES = 2;
const HOME_VIEW_SLOWEST = 5;
const PROGRESS_SLOW_COVER_MS = 1000;

const COVER_COLUMNS = `c.cover_id, c.work_id, c.url, c.is_primary,
  c.width_px, c.height_px`;

const VIEWER_PINS: readonly [RegExp, string][] = [
  [
    /(\.related\("ownReadthroughs", \(readthrough\) =>\s*readthrough)/,
    '$1.where("user_id", "=", userId)',
  ],
  [
    /(\.related\("ownLastFinishedReadthrough", \(readthrough\) =>\s*readthrough)/,
    '$1.where("user_id", "=", userId)',
  ],
  [
    /(\.related\("ownWantToRead", \(wantToRead\) =>\s*wantToRead)/,
    '$1.where("user_id", "=", userId)',
  ],
];

const toolDir = path.dirname(fileURLToPath(import.meta.url));
const packageDir = path.dirname(toolDir);
const repoDir = path.resolve(packageDir, '../..');
const options = parseArgs(process.argv.slice(2));

await assertInput(options.replica, 'replica');
await assertInput(options.queriesDir, 'query bundle');

const workDir = await mkdtemp(path.join(tmpdir(), 'replica-advance-perf-'));
const clone = path.join(workDir, 'replica.db');
const bundleDir = await mkdtemp(
  path.join(packageDir, '.replica-advance-perf-'),
);
const bundle = path.join(bundleDir, 'runtime.cjs');
let cleanupOnExit = true;
process.once('exit', () => {
  if (cleanupOnExit) {
    rmSync(bundleDir, {recursive: true, force: true});
    if (!options.keepClone) {
      rmSync(workDir, {recursive: true, force: true});
    }
  }
});

try {
  const cloneStarted = performance.now();
  await copyOnWriteClone(options.replica, clone);
  process.stderr.write(
    `Created copy-on-write replica clone in ${formatMs(performance.now() - cloneStarted)}\n`,
  );

  await buildRuntime(options, bundle);
  const runtime = (await import(pathToFileURL(bundle).href)) as Runtime;
  const results =
    options.workload === 'homeview'
      ? runHomeViewWorkload(runtime, clone, options)
      : runWorkload(runtime, clone, options);

  if (options.json) {
    process.stdout.write(`${JSON.stringify(results, null, 2)}\n`);
  } else if (options.workload === 'homeview') {
    printHomeViewResults(results as HomeViewResult[]);
  } else {
    printResults(results as Result[]);
  }
} finally {
  await rm(bundleDir, {recursive: true, force: true});
  if (options.keepClone) {
    process.stderr.write(`Kept replica clone at ${clone}\n`);
  } else {
    await rm(workDir, {recursive: true, force: true});
  }
  cleanupOnExit = false;
}

function runWorkload(
  runtime: Runtime,
  replica: string,
  opts: Options,
): Result[] {
  const lc = runtime.createSilentLogContext();
  const setupDB = new runtime.Database(lc, replica);
  addMissingReplicaColumns(setupDB);
  const users = sampleUsers(setupDB, opts.users);
  const covers = sampleCovers(setupDB, opts.batchSize);
  const coverRequests = makeCoverRequests(setupDB, opts.users);
  setupDB.close();

  process.stderr.write(
    `Sampled ${users.length} high-row-count users and ${covers.length} referenced covers\n`,
  );

  const results: Result[] = [];
  const executionUsers = opts.workload === 'cover' ? users.slice(0, 1) : users;
  for (const userID of executionUsers) {
    const requests =
      opts.workload === 'background'
        ? runtime
            .backgroundQueriesRequests(userID, Date.now())
            .map(request => ({
              args: request.args,
              name: request.query.queryName,
            }))
        : coverRequests;
    const expectedNames =
      opts.workload === 'background'
        ? new Set(runtime.BACKGROUND_QUERIES.map(q => q.name))
        : undefined;
    for (const request of requests) {
      const queryName = request.name;
      expectedNames?.delete(queryName);
      if (opts.queryPattern && !opts.queryPattern.test(queryName)) {
        continue;
      }
      results.push(
        measureQuery(runtime, replica, userID, queryName, request.args, covers),
      );
    }
    if (expectedNames && expectedNames.size > 0) {
      throw new Error(
        `Background request generation missed: ${[...expectedNames].join(', ')}`,
      );
    }
  }
  return results;
}

function measureQuery(
  runtime: Runtime,
  replica: string,
  userID: string,
  queryName: string,
  args: unknown,
  covers: readonly Row[],
): Result {
  const db = new runtime.Database(runtime.createSilentLogContext(), replica);
  let view: View | undefined;
  try {
    const definition = runtime.queries[queryName];
    if (!definition) {
      throw new Error(`Server query is not registered: ${queryName}`);
    }
    const query = definition.fn({
      args,
      ctx: {subject: {authenticatedUserId: userID}},
    });
    const ast = runtime.asQueryInternals(query).ast;
    const readsWorkCovers = JSON.stringify(ast).includes('work_covers');
    const delegate = runtime.newQueryDelegate(
      runtime.createSilentLogContext(),
      runtime.testLogConfig,
      db,
      runtime.schema,
    );

    const hydrateStarted = performance.now();
    view = delegate.materialize(query);
    const hydrateMs = performance.now() - hydrateStarted;
    const source = delegate.getSource('catalog.work_covers');

    db.exec('BEGIN');
    const pushStarted = performance.now();
    for (let i = 0; i < covers.length; i++) {
      const oldRow = covers[i];
      const newRow = {
        ...oldRow,
        background_color_hex: `#${i.toString(16).padStart(6, '0').slice(-6)}`,
      };
      for (const _ of source.push(
        runtime.makeSourceChangeEdit(newRow, oldRow),
      )) {
        // Exhaust the cooperative stream to advance every connected pipeline.
      }
    }
    const pushMs = performance.now() - pushStarted;
    const flushStarted = performance.now();
    view.flush();
    const flushMs = performance.now() - flushStarted;
    db.exec('COMMIT');

    return {
      userID,
      query: queryName,
      readsWorkCovers,
      hydrateMs,
      pushMs,
      flushMs,
      totalMs: pushMs + flushMs,
    };
  } catch (error) {
    try {
      db.exec('ROLLBACK');
    } catch {
      // There is no open transaction when setup or hydration failed.
    }
    return {
      userID,
      query: queryName,
      readsWorkCovers: false,
      hydrateMs: 0,
      pushMs: 0,
      flushMs: 0,
      totalMs: 0,
      error:
        error instanceof Error ? (error.stack ?? error.message) : String(error),
    };
  } finally {
    view?.destroy();
    db.close();
  }
}

type HomeViewResult = {
  scenario: string;
  pass: number;
  userID: string;
  viewerPin: boolean;
  covers: number;
  primaryCovers: number;
  hydrateMs: number;
  pushMs: number;
  flushMs: number;
  slowest: {coverID: string; workID: string; ms: number}[];
  error?: string | undefined;
};

type HomeViewScenario = {
  name: string;
  covers: readonly Row[];
};

// Reproduces runaway push in Margins' `homeView`. A `work_covers` edit reaches
// the query through `readthroughs.work`, `readthroughs.preferredCover`,
// `relatedItems.relatedWork` and `wantToReadSeries.works.work`. Each join
// fetches its parents by the join key alone, so the push scans every user's
// readthroughs of the edited work, even though only the viewer's reach the
// output. The `backfill` scenario is the first batch of the real backfill
// (covers in `cover_id` order). The `hot` scenario is the primary covers of the
// most-read works, which that backfill eventually reaches.
function runHomeViewWorkload(
  runtime: Runtime,
  replica: string,
  opts: Options,
): HomeViewResult[] {
  const lc = runtime.createSilentLogContext();
  const setupDB = new runtime.Database(lc, replica);
  addMissingReplicaColumns(setupDB);
  const userID = opts.user ?? sampleHomeViewUser(setupDB);
  const scenarios: HomeViewScenario[] = [
    {name: 'backfill', covers: sampleBackfillCovers(setupDB, opts.batchSize)},
    {name: 'hot', covers: sampleHotCovers(setupDB, opts.hotWorks)},
  ];
  setupDB.close();

  process.stderr.write(
    `homeView viewer ${userID}; ${opts.viewerPin ? 'with' : 'without'} viewer pin\n`,
  );

  const results: HomeViewResult[] = [];
  for (let pass = 1; pass <= HOME_VIEW_PASSES; pass++) {
    for (const scenario of scenarios) {
      const result = measureHomeView(runtime, replica, userID, scenario, opts);
      result.pass = pass;
      results.push(result);
      process.stderr.write(
        `pass ${pass} ${scenario.name}: push ${formatMs(result.pushMs)}` +
          ` (hydrate ${formatMs(result.hydrateMs)})\n`,
      );
    }
  }
  return results;
}

function measureHomeView(
  runtime: Runtime,
  replica: string,
  userID: string,
  scenario: HomeViewScenario,
  opts: Options,
): HomeViewResult {
  const db = new runtime.Database(runtime.createSilentLogContext(), replica);
  const result: HomeViewResult = {
    scenario: scenario.name,
    pass: 0,
    userID,
    viewerPin: opts.viewerPin,
    covers: scenario.covers.length,
    primaryCovers: scenario.covers.filter(c => c.is_primary).length,
    hydrateMs: 0,
    pushMs: 0,
    flushMs: 0,
    slowest: [],
  };
  let view: View | undefined;
  let inTransaction = false;
  try {
    const definition = runtime.queries['homeView'];
    if (!definition) {
      throw new Error('Server query is not registered: homeView');
    }
    const query = definition.fn({
      args: userID,
      ctx: {subject: {authenticatedUserId: userID}},
    });
    const delegate = runtime.newQueryDelegate(
      runtime.createSilentLogContext(),
      runtime.testLogConfig,
      db,
      runtime.schema,
    );

    const hydrateStarted = performance.now();
    view = delegate.materialize(query);
    result.hydrateMs = performance.now() - hydrateStarted;
    process.stderr.write(
      `  ${scenario.name}: hydrated in ${formatMs(result.hydrateMs)}\n`,
    );
    const source = delegate.getSource('catalog.work_covers');

    db.exec('BEGIN');
    inTransaction = true;
    const timings: {coverID: string; workID: string; ms: number}[] = [];
    for (let i = 0; i < scenario.covers.length; i++) {
      const oldRow = scenario.covers[i];
      const newRow = {
        ...oldRow,
        background_color_hex: `#${i.toString(16).padStart(6, '0').slice(-6)}`,
      };
      const started = performance.now();
      for (const _ of source.push(
        runtime.makeSourceChangeEdit(newRow, oldRow),
      )) {
        // Exhaust the cooperative stream to advance the pipeline.
      }
      const ms = performance.now() - started;
      if (ms > PROGRESS_SLOW_COVER_MS) {
        process.stderr.write(
          `  ${scenario.name}: cover ${i + 1}/${scenario.covers.length}` +
            ` (work ${String(oldRow.work_id).slice(0, 8)}) took ${formatMs(ms)}\n`,
        );
      }
      result.pushMs += ms;
      timings.push({
        coverID: String(oldRow.cover_id),
        workID: String(oldRow.work_id),
        ms,
      });
    }
    const flushStarted = performance.now();
    view.flush();
    result.flushMs = performance.now() - flushStarted;
    result.slowest = timings
      .toSorted((a, b) => b.ms - a.ms)
      .slice(0, HOME_VIEW_SLOWEST);
  } catch (error) {
    result.error =
      error instanceof Error ? (error.stack ?? error.message) : String(error);
  } finally {
    view?.destroy();
    // Roll back so every scenario and pass starts from the same replica.
    if (inTransaction) {
      db.exec('ROLLBACK');
    }
    db.close();
  }
  return result;
}

// A viewer whose rows reach every branch the cover edits push through:
// in-progress readthroughs, a finished readthrough with a real end date (so
// the partitioned limit(1) has a bound) and want-to-read rows.
function sampleHomeViewUser(db: Database): string {
  const [row] = db
    .prepare(
      `SELECT p.user_id FROM 'userspace.profiles' AS p
       WHERE EXISTS (SELECT 1 FROM 'userspace.readthroughs' AS r
                     WHERE r.user_id = p.user_id AND r.status = 'in_progress')
         AND EXISTS (SELECT 1 FROM 'userspace.readthroughs' AS r
                     WHERE r.user_id = p.user_id AND r.status = 'finished'
                       AND r.end_date <= '9999-12-31')
         AND EXISTS (SELECT 1 FROM 'userspace.want_to_read' AS w
                     WHERE w.user_id = p.user_id)
       LIMIT 1`,
    )
    .all();
  if (!row) {
    throw new Error('Replica has no viewer that reaches every homeView branch');
  }
  return String(row.user_id);
}

function sampleBackfillCovers(db: Database, count: number): Row[] {
  return db
    .prepare(
      `SELECT ${COVER_COLUMNS} FROM 'catalog.work_covers' AS c
       ORDER BY c.cover_id LIMIT ?`,
    )
    .all(count)
    .map(toZeroCoverRow);
}

function sampleHotCovers(db: Database, works: number): Row[] {
  return db
    .prepare(
      `WITH hot AS (
         SELECT work_id, count(*) AS readthroughs
         FROM 'userspace.readthroughs'
           INDEXED BY 'userspace.idx_readthroughs_work_id'
         GROUP BY work_id ORDER BY readthroughs DESC LIMIT ?)
       SELECT ${COVER_COLUMNS} FROM hot
       JOIN 'catalog.work_covers' AS c ON c.work_id = hot.work_id
       WHERE c.is_primary
       ORDER BY hot.readthroughs DESC`,
    )
    .all(works)
    .map(toZeroCoverRow);
}

// SQLite stores booleans as 0/1. Pushed rows must use Zero's types, or the
// `is_primary = true` filters drop every edit before it reaches a join.
function toZeroCoverRow(row: Row): Row {
  return {...row, is_primary: row.is_primary === 1};
}

// The user-level workaround: repeat the root's `user_id = userId` on each
// user-owned subquery so push fetches use the (user_id, …) indexes.
function pinHomeViewToViewer(source: string): string {
  let pinned = source;
  for (const [pattern, replacement] of VIEWER_PINS) {
    if (!pattern.test(pinned)) {
      throw new Error(`Could not find homeView subquery ${pattern}`);
    }
    pinned = pinned.replace(pattern, replacement);
  }
  return pinned;
}

function printHomeViewResults(results: readonly HomeViewResult[]): void {
  const rows = results.map(result => ({
    pass: result.pass,
    scenario: result.scenario,
    covers: result.covers,
    primary: result.primaryCovers,
    hydrate_ms: result.hydrateMs.toFixed(1),
    push_ms: result.pushMs.toFixed(1),
    flush_ms: result.flushMs.toFixed(1),
    push_per_hydrate: `${(result.pushMs / Math.max(result.hydrateMs, 0.001)).toFixed(0)}x`,
    slowest_cover_ms: result.slowest[0]?.ms.toFixed(1) ?? '',
    slowest_work: result.slowest[0]?.workID.slice(0, 8) ?? '',
    error: result.error?.split('\n')[0] ?? '',
  }));
  const columns = Object.keys(rows[0] ?? {});
  process.stdout.write(
    [
      columns.join('\t'),
      ...rows.map(row =>
        columns.map(column => row[column as keyof typeof row]).join('\t'),
      ),
    ].join('\n') + '\n',
  );
}

function addMissingReplicaColumns(db: Database): void {
  addColumnIfMissing(db, 'catalog.work_covers', 'background_color_hex', 'TEXT');
  addColumnIfMissing(db, 'catalog.work_covers', 'width_px', 'REAL');
  addColumnIfMissing(db, 'catalog.work_covers', 'height_px', 'REAL');
  addColumnIfMissing(db, 'catalog.contributors', 'num_works', 'REAL DEFAULT 0');
  addColumnIfMissing(db, 'catalog.series', 'num_works', 'REAL DEFAULT 0');
  addColumnIfMissing(db, 'userspace.profiles', 'member_since', 'REAL');
  addColumnIfMissing(db, 'userspace.profiles', 'pfp_url', 'TEXT');
}

function addColumnIfMissing(
  db: Database,
  table: string,
  column: string,
  definition: string,
): void {
  const exists = db
    .prepare(`PRAGMA table_info('${table}')`)
    .all()
    .some(row => row.name === column);
  if (!exists) {
    db.exec(`ALTER TABLE '${table}' ADD COLUMN ${column} ${definition}`);
  }
}

function sampleUsers(db: Database, count: number): string[] {
  return db
    .prepare(
      `SELECT user_id, count(*) AS row_count
       FROM 'userspace.works'
       GROUP BY user_id
       ORDER BY row_count DESC
       LIMIT ?`,
    )
    .all(count)
    .map(row => String(row.user_id));
}

function sampleCovers(db: Database, count: number): Row[] {
  return db
    .prepare(
      `SELECT c.cover_id, c.work_id, c.url, c.is_primary,
              c.width_px, c.height_px
       FROM 'catalog.work_covers' AS c
       JOIN 'catalog.work_isbn13s' AS i
         ON i.work_id = c.work_id AND i.cover_id = c.cover_id
       WHERE i.cover_id IS NOT NULL
       LIMIT ?`,
    )
    .all(count);
}

function makeCoverRequests(db: Database, count: number): WorkloadRequest[] {
  const editions = db
    .prepare(
      `SELECT i.isbn13, i.work_id
       FROM 'catalog.work_isbn13s' AS i
       WHERE i.cover_id IS NOT NULL
       LIMIT ?`,
    )
    .all(Math.max(count, 10));
  if (editions.length === 0) {
    throw new Error('Replica has no ISBN rows with edition covers');
  }
  const requests: WorkloadRequest[] = [];
  for (const edition of editions.slice(0, count)) {
    requests.push(
      {name: 'workById', args: edition.work_id},
      {name: 'workCoversByWorkId', args: edition.work_id},
      {name: 'offersByWorkId', args: edition.work_id},
      {name: 'workByISBN13', args: edition.isbn13},
      {name: 'offerByISBN13', args: edition.isbn13},
    );
  }
  requests.push({
    name: 'offersByISBN13s',
    args: editions.slice(0, 10).map(row => row.isbn13),
  });
  return requests;
}

async function buildRuntime(opts: Options, outfile: string): Promise<void> {
  const entry = runtimeEntry(opts.queriesDir);
  await build({
    stdin: {
      contents: entry,
      loader: 'ts',
      resolveDir: opts.queriesDir,
      sourcefile: 'perf-entry.ts',
    },
    outfile,
    bundle: true,
    format: 'cjs',
    platform: 'node',
    target: 'node24',
    define: {'import.meta.env.VITEST': 'false'},
    sourcemap: 'inline',
    plugins: [bundleImportsPlugin(opts)],
    logLevel: 'warning',
  });
}

function bundleImportsPlugin(opts: Options): Plugin {
  const requireFromZqlite = createRequire(
    path.join(repoDir, 'packages/zqlite/package.json'),
  );
  const zeroMod = path.join(repoDir, 'packages/zero/src/zero.ts');
  const schemaModules = domainSchemaModules(opts.queriesDir);
  const backgroundFile = path.join(
    opts.queriesDir,
    'packages/sync/composition/src/background-queries.ts',
  );
  return {
    name: 'bundle-imports',
    setup(build): void {
      build.onResolve({filter: ZERO_SQLITE_IMPORT}, () => ({
        path: requireFromZqlite.resolve('@rocicorp/zero-sqlite3'),
        external: true,
      }));
      build.onResolve({filter: ZERO_IMPORT}, () => ({path: zeroMod}));
      build.onResolve({filter: EFFECT_IMPORT}, () => ({
        path: 'effect',
        namespace: 'bundle-shim',
      }));
      build.onResolve({filter: ZERO_CONTEXT_IMPORT}, () => ({
        path: 'empty',
        namespace: 'bundle-shim',
      }));
      build.onResolve({filter: USER_ID_IMPORT}, () => ({
        path: 'user-id',
        namespace: 'bundle-shim',
      }));
      build.onResolve({filter: EFFECT_SCHEMA_IMPORT}, () => ({
        path: 'effect-schema',
        namespace: 'bundle-shim',
      }));
      build.onResolve({filter: COMMERCE_CORE_IMPORT}, () => ({
        path: 'commerce-core',
        namespace: 'bundle-shim',
      }));
      build.onResolve({filter: ISBN_IMPORT}, () => ({
        path: 'isbn',
        namespace: 'bundle-shim',
      }));
      build.onResolve({filter: SOCIAL_CONNECTIONS_IMPORT}, () => ({
        path: 'social-connections',
        namespace: 'bundle-shim',
      }));
      build.onResolve({filter: ZERO_SCHEMA_IMPORT}, args => {
        const files = schemaModules.get(args.path);
        if (!files) {
          throw new Error(`No schema-only mapping for ${args.path}`);
        }
        const includeQueries =
          path.resolve(args.importer) === backgroundFile ||
          args.importer.includes('queries.ts');
        return {
          path: JSON.stringify({files, includeQueries}),
          namespace: 'bundle-domain',
        };
      });
      build.onLoad({filter: ALL_PATHS, namespace: 'bundle-domain'}, args => {
        const value = JSON.parse(args.path) as {
          files: {schemas: string[]; queries: string[]};
          includeQueries: boolean;
        };
        const files = value.includeQueries
          ? [...value.files.schemas, ...value.files.queries]
          : value.files.schemas;
        return {
          contents: files
            .map(file => `export * from ${JSON.stringify(file)};`)
            .join('\n'),
          loader: 'ts',
          resolveDir: opts.queriesDir,
        };
      });
      build.onLoad({filter: ALL_PATHS, namespace: 'bundle-shim'}, args => ({
        contents: shim(args.path),
        loader: 'ts',
      }));
      build.onLoad({filter: HOME_QUERIES_PATH}, async args => {
        const source = await readFile(args.path, 'utf8');
        return {
          contents: opts.viewerPin ? pinHomeViewToViewer(source) : source,
          loader: 'ts',
        };
      });
      build.onLoad({filter: CATALOG_SCHEMA_PATH}, async args => {
        let source = await readFile(args.path, 'utf8');
        const legacy = `sourceField: ["cover_id"],\n      destField: ["cover_id"],\n      destSchema: workCoversTable`;
        const fixed = `sourceField: ["work_id", "cover_id"],\n      destField: ["work_id", "cover_id"],\n      destSchema: workCoversTable`;
        if (opts.legacyCoverJoin && source.includes(fixed)) {
          source = source.replace(fixed, legacy);
        } else if (!opts.legacyCoverJoin && source.includes(legacy)) {
          source = source.replace(legacy, fixed);
        } else if (!source.includes(opts.legacyCoverJoin ? legacy : fixed)) {
          throw new Error('Could not find the workISBN13s cover relationship');
        }
        return {contents: source, loader: 'ts'};
      });
    },
  };
}

function domainSchemaModules(
  root: string,
): Map<string, {schemas: string[]; queries: string[]}> {
  const from = (...parts: string[]) => path.join(root, ...parts);
  const domain = (
    packagePath: string,
    schemas: string[],
    queries: string[],
  ): [string, {schemas: string[]; queries: string[]}] => [
    `@workspace/${packagePath}`,
    {
      schemas: schemas.map(file => from(file)),
      queries: queries.map(file => from(file)),
    },
  ];
  return new Map([
    domain(
      'authentication-zero-schema',
      ['packages/authentication/zero-schema/src/index.ts'],
      [],
    ),
    domain(
      'book-clubs-zero-schema',
      ['packages/book-clubs/zero-schema/src/schema.ts'],
      ['packages/book-clubs/zero-schema/src/queries.ts'],
    ),
    domain(
      'catalog-zero-schema',
      ['packages/catalog/zero-schema/src/schema.ts'],
      ['packages/catalog/zero-schema/src/queries.ts'],
    ),
    domain(
      'commerce-zero-schema',
      ['packages/commerce/zero-schema/src/schema.ts'],
      [
        'packages/commerce/zero-schema/src/queries.ts',
        'packages/commerce/zero-schema/src/server-queries.ts',
      ],
    ),
    domain(
      'contacts-zero-schema',
      ['packages/contacts/zero-schema/src/schema.ts'],
      ['packages/contacts/zero-schema/src/queries.ts'],
    ),
    domain(
      'home-zero-schema',
      ['packages/home/zero-schema/src/schema.ts'],
      ['packages/home/zero-schema/src/queries.ts'],
    ),
    domain(
      'library-zero-schema',
      ['packages/library/zero-schema/src/schema.ts'],
      ['packages/library/zero-schema/src/queries.ts'],
    ),
    domain(
      'missing-isbn-report-zero-schema',
      ['packages/missing-isbn-report/zero-schema/src/schema.ts'],
      [],
    ),
    domain(
      'notifications-zero-schema',
      [
        'packages/notifications/zero-schema/src/schema.ts',
        'packages/notifications/zero-schema/src/catalog-schema.ts',
        'packages/notifications/zero-schema/src/inbox-schema.ts',
        'packages/notifications/zero-schema/src/push-token-schema.ts',
      ],
      [
        'packages/notifications/zero-schema/src/queries.ts',
        'packages/notifications/zero-schema/src/catalog-queries.ts',
        'packages/notifications/zero-schema/src/inbox-actor-view-queries.ts',
        'packages/notifications/zero-schema/src/inbox-queries.ts',
        'packages/notifications/zero-schema/src/push-token-queries.ts',
      ],
    ),
    domain(
      'reading-session-background-zero-schema',
      ['packages/reading-session-background/zero-schema/src/schema.ts'],
      ['packages/reading-session-background/zero-schema/src/queries.ts'],
    ),
    domain(
      'request-book-zero-schema',
      ['packages/request-book/zero-schema/src/schema.ts'],
      ['packages/request-book/zero-schema/src/queries.ts'],
    ),
    domain(
      'search-by-vibes-presets-zero-schema',
      ['packages/search-by-vibes-presets/zero-schema/src/schema.ts'],
      ['packages/search-by-vibes-presets/zero-schema/src/queries.ts'],
    ),
    domain(
      'search-by-vibes-zero-schema',
      ['packages/search-by-vibes/zero-schema/src/schema.ts'],
      ['packages/search-by-vibes/zero-schema/src/queries.ts'],
    ),
    domain(
      'social-connections-zero-schema',
      [
        'packages/social-connections/zero-schema/src/schema.ts',
        'packages/social-connections/zero-schema/src/profile-visibility.ts',
      ],
      [
        'packages/social-connections/zero-schema/src/queries.ts',
        'packages/social-connections/zero-schema/src/profile-content-server-queries.ts',
        'packages/social-connections/zero-schema/src/reading-activity-server-queries.ts',
      ],
    ),
    domain(
      'story-zero-schema',
      ['packages/story/zero-schema/src/schema.ts'],
      ['packages/story/zero-schema/src/queries.ts'],
    ),
    domain(
      'user-profile-content-zero-schema',
      ['packages/user-profile/content-zero-schema/src/schema.ts'],
      ['packages/user-profile/content-zero-schema/src/queries.ts'],
    ),
    domain(
      'user-profile-zero-schema',
      ['packages/user-profile/zero-schema/src/schema.ts'],
      ['packages/user-profile/zero-schema/src/queries.ts'],
    ),
  ]);
}

function shim(name: string): string {
  switch (name) {
    case 'empty':
      return 'export {}';
    case 'effect':
      return `
        const validator = {
          '~standard': {version: 1, vendor: 'replica-advance-perf', validate: value => ({value})},
          pipe: () => validator,
          annotations: () => validator,
        };
        const constructor = () => validator;
        export const Schema = new Proxy({}, {
          get: (_target, property) => property === 'String' || property === 'Number' || property === 'Boolean' || property === 'Null'
            ? validator
            : constructor,
        });
      `;
    case 'user-id':
      return `import {Schema} from 'effect'; export const UserId = Schema.String;`;
    case 'effect-schema':
      return `import {Schema} from 'effect'; export const optionalExact = schema => Schema.optionalWith(schema, {exact: true});`;
    case 'commerce-core':
      return `export const SELLABLE_PRODUCT_TYPES = ['HARDCOVER', 'PAPERBACK', 'MASS_MARKET_PAPERBACK', 'AUDIOBOOK', 'EBOOK'];`;
    case 'isbn':
      return `export const toPostgresISBN13 = value => { const s = value.replaceAll('-', ''); if (!/^\\d{13}$/.test(s)) throw new Error('invalid ISBN-13'); return s.slice(0,3)+'-'+s.slice(3,4)+'-'+s.slice(4,7)+'-'+s.slice(7,12)+'-'+s.slice(12); };`;
    case 'social-connections':
      return `export const CONNECTIONS_LIST_CHUNK = 50; export const CONNECTIONS_LIST_MAX = 500; export const FRIEND_RAIL_FOLLOWING_CHUNK = 25; export const FRIEND_RAIL_FOLLOWING_MAX = 100;`;
    default:
      throw new Error(`Unknown shim: ${name}`);
  }
}

function runtimeEntry(root: string): string {
  const file = (...parts: string[]) =>
    JSON.stringify(path.join(root, ...parts));
  return `
    import {defineQueries} from '@rocicorp/zero';
    import {schema} from ${file('packages/sync/composition/src/schema.ts')};
    import {BACKGROUND_QUERIES, backgroundQueriesRequests} from ${file('packages/sync/composition/src/background-queries.ts')};
    import {authenticationServerQueries} from ${file('packages/authentication/zero-schema/src/index.ts')};
    import {bookClubServerQueries} from ${file('packages/book-clubs/zero-schema/src/queries.ts')};
    import {catalogQueries, contributorQueries} from ${file('packages/catalog/zero-schema/src/queries.ts')};
    import {commerceServerQueries} from ${file('packages/commerce/zero-schema/src/server-queries.ts')};
    import {contactsServerQueries} from ${file('packages/contacts/zero-schema/src/queries.ts')};
    import {homeServerQueries} from ${file('packages/home/zero-schema/src/queries.ts')};
    import {libraryServerQueries} from ${file('packages/library/zero-schema/src/queries.ts')};
    import {notificationCatalogQueries} from ${file('packages/notifications/zero-schema/src/catalog-queries.ts')};
    import {inboxActorServerQueries} from ${file('packages/notifications/zero-schema/src/inbox-actor-view-queries.ts')};
    import {inboxNotificationServerQueries} from ${file('packages/notifications/zero-schema/src/inbox-queries.ts')};
    import {pushTokenServerQueries} from ${file('packages/notifications/zero-schema/src/push-token-queries.ts')};
    import {notificationPreferenceServerQueries} from ${file('packages/notifications/zero-schema/src/queries.ts')};
    import {readingSessionBackgroundServerQueries} from ${file('packages/reading-session-background/zero-schema/src/queries.ts')};
    import {requestBookServerQueries} from ${file('packages/request-book/zero-schema/src/queries.ts')};
    import {searchByVibesPresetsQueries} from ${file('packages/search-by-vibes-presets/zero-schema/src/queries.ts')};
    import {searchByVibesServerQueries} from ${file('packages/search-by-vibes/zero-schema/src/queries.ts')};
    import {profileContentServerQueries} from ${file('packages/social-connections/zero-schema/src/profile-content-server-queries.ts')};
    import {socialConnectionServerQueries, userProfileServerQueries} from ${file('packages/social-connections/zero-schema/src/queries.ts')};
    import {storyServerQueries} from ${file('packages/story/zero-schema/src/queries.ts')};
    import {importResultServerQueries} from ${file('packages/sync/composition/src/import-result-queries.ts')};
    import {issuesServerQueries} from ${file('packages/sync/composition/src/issues-queries.ts')};
    import {Database} from ${JSON.stringify(path.join(repoDir, 'packages/zqlite/src/db.ts'))};
    import {newQueryDelegate} from ${JSON.stringify(path.join(repoDir, 'packages/zqlite/src/test/source-factory.ts'))};
    import {asQueryInternals} from ${JSON.stringify(path.join(repoDir, 'packages/zql/src/query/query-internals.ts'))};
    import {makeSourceChangeEdit} from ${JSON.stringify(path.join(repoDir, 'packages/zql/src/ivm/source.ts'))};
    import {createSilentLogContext} from ${JSON.stringify(path.join(repoDir, 'packages/shared/src/logging-test-utils.ts'))};
    import {testLogConfig} from ${JSON.stringify(path.join(repoDir, 'packages/otel/src/test-log-config.ts'))};

    const queries = defineQueries({
      ...commerceServerQueries,
      ...homeServerQueries,
      ...libraryServerQueries,
      ...storyServerQueries,
      ...notificationPreferenceServerQueries,
      ...inboxNotificationServerQueries,
      ...inboxActorServerQueries,
      ...pushTokenServerQueries,
      ...userProfileServerQueries,
      ...profileContentServerQueries,
      ...socialConnectionServerQueries,
      ...catalogQueries,
      ...contributorQueries,
      ...searchByVibesPresetsQueries,
      ...authenticationServerQueries,
      ...searchByVibesServerQueries,
      ...notificationCatalogQueries,
      ...bookClubServerQueries,
      ...importResultServerQueries,
      ...issuesServerQueries,
      ...contactsServerQueries,
      ...readingSessionBackgroundServerQueries,
      ...requestBookServerQueries,
    });

    export {
      BACKGROUND_QUERIES,
      Database,
      asQueryInternals,
      backgroundQueriesRequests,
      createSilentLogContext,
      makeSourceChangeEdit,
      newQueryDelegate,
      queries,
      schema,
      testLogConfig,
    };
  `;
}

function printResults(results: readonly Result[]): void {
  const sorted = results.toSorted((a, b) => b.totalMs - a.totalMs);
  const rows = sorted.map(result => ({
    user: result.userID.slice(0, 8),
    query: result.query,
    covers: result.readsWorkCovers ? 'yes' : 'no',
    hydrate_ms: result.hydrateMs.toFixed(1),
    push_ms: result.pushMs.toFixed(1),
    flush_ms: result.flushMs.toFixed(1),
    total_ms: result.totalMs.toFixed(1),
    error: result.error?.split('\n')[0] ?? '',
  }));
  const columns = Object.keys(rows[0] ?? {});
  const output = [
    columns.join('\t'),
    ...rows.map(row =>
      columns.map(column => row[column as keyof typeof row]).join('\t'),
    ),
  ].join('\n');
  process.stdout.write(`${output}\n`);
}

function parseArgs(args: readonly string[]): Options {
  const values = new Map<string, string>();
  const flags = new Set<string>();
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (!arg.startsWith('--')) {
      throw new Error(`Unexpected argument: ${arg}`);
    }
    const [key, inlineValue] = arg.slice(2).split('=', 2);
    if (inlineValue !== undefined) {
      values.set(key, inlineValue);
    } else if (args[i + 1] && !args[i + 1].startsWith('--')) {
      values.set(key, args[++i]);
    } else {
      flags.add(key);
    }
  }
  if (flags.has('help')) {
    process.stdout.write(
      `Usage: pnpm --filter zql-benchmarks advance:perf -- --replica PATH --queries-dir PATH [options]\n\nOptions:\n  --replica PATH           SQLite replica file (required)\n  --queries-dir PATH       Exported query bundle (required)\n  --workload NAME          background (default), cover or homeview\n  --users N                Users or parameter samples to test (default: 3)\n  --batch-size N           Referenced work covers per write (default: 500)\n  --query REGEXP           Only matching query names\n  --legacy-cover-join      Keep the cover_id-only relationship for comparison\n  --keep-clone             Keep the disposable replica clone\n  --hot-works N            homeview: most-read works whose primary covers form the hot batch (default: 5)\n  --viewer-pin             homeview: add the redundant user_id filters to homeView's user-owned subqueries\n  --user ID                homeview: viewer to hydrate (default: first with in-progress, finished and want-to-read rows)\n  --json                   Emit JSON results\n`,
    );
    process.exit(0);
  }
  const users = positiveInteger(values.get('users') ?? '3', '--users');
  const batchSize = positiveInteger(
    values.get('batch-size') ?? '500',
    '--batch-size',
  );
  const workload = values.get('workload') ?? 'background';
  if (
    workload !== 'background' &&
    workload !== 'cover' &&
    workload !== 'homeview'
  ) {
    throw new Error('--workload must be background, cover or homeview');
  }
  return {
    replica: path.resolve(requiredValue(values, 'replica')),
    queriesDir: path.resolve(requiredValue(values, 'queries-dir')),
    users,
    batchSize,
    queryPattern: values.has('query')
      ? new RegExp(values.get('query') as string)
      : undefined,
    legacyCoverJoin: flags.has('legacy-cover-join'),
    keepClone: flags.has('keep-clone'),
    json: flags.has('json'),
    workload,
    hotWorks: positiveInteger(values.get('hot-works') ?? '5', '--hot-works'),
    viewerPin: flags.has('viewer-pin'),
    user: values.get('user'),
  };
}

function requiredValue(values: Map<string, string>, option: string): string {
  const value = values.get(option);
  if (value === undefined) {
    throw new Error(`--${option} is required`);
  }
  return value;
}

function positiveInteger(value: string, option: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${option} must be a positive integer`);
  }
  return parsed;
}

async function assertInput(file: string, label: string): Promise<void> {
  try {
    await stat(file);
  } catch {
    throw new Error(`${label} does not exist: ${file}`);
  }
}

async function copyOnWriteClone(
  source: string,
  destination: string,
): Promise<void> {
  if (process.platform !== 'darwin') {
    await copyFile(source, destination, fsConstants.COPYFILE_FICLONE_FORCE);
    return;
  }
  await new Promise<void>((resolve, reject) => {
    const child = spawn('/bin/cp', ['-c', source, destination], {
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    let stderr = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', chunk => {
      stderr += chunk;
    });
    child.once('error', reject);
    child.once('close', code => {
      if (code === 0) {
        resolve();
      } else {
        reject(
          new Error(`cp -c failed with exit code ${code}: ${stderr.trim()}`),
        );
      }
    });
  });
}

function formatMs(ms: number): string {
  return `${ms.toFixed(1)}ms`;
}
