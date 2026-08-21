import {spawnSync} from 'node:child_process';
import {existsSync, realpathSync} from 'node:fs';
import {mkdir, readdir} from 'node:fs/promises';
import {basename, dirname, isAbsolute, resolve} from 'node:path';
import {performance} from 'node:perf_hooks';
import {pathToFileURL} from 'node:url';

// CLI for comparing planner scenario performance across two local Zero checkouts.
//
// Common commands:
//
//   npm --workspace=zql-benchmarks run bench:scenario -- --left origin/main --right . --scenario student-membership-mixed-or
//   npm --workspace=zql-benchmarks run bench:scenario -- --left 9587e11 --right 04dc506 --scenario permission-and-class-filter-intersection
//   npm --workspace=zql-benchmarks run bench:scenario -- --left . --right . --left-scenario parent-or-exists-union-roots --right-scenario student-membership-mixed-or
//
// The left and right targets can be local git refs, commits, branches, or
// filesystem paths. Git refs are resolved with the local object database only:
// no network fetch happens here. Ref targets are checked out into detached
// worktrees under .tmp/query-scenario-worktrees so the current checkout can stay
// dirty while the benchmark imports runtime modules from each side.
//
// Use --format json when you need exact SQL shapes and planner debug text for a
// PR comment. Run with --help for the same usage information at the terminal.
//
// Scenario definitions load from this checkout by default, then run against
// both targets. That is intentional for new scenarios that older commits do not
// have, but it means this is not a pure branch-to-branch test of old scenario
// files unless --scenario-source points somewhere else.

type Format = 'json' | 'markdown';

type Options = {
  readonly left: string;
  readonly right: string;
  readonly scenarioSource: string;
  readonly scenario: string | undefined;
  readonly leftScenario: string | undefined;
  readonly rightScenario: string | undefined;
  readonly iterations: number;
  readonly warmups: number;
  readonly format: Format;
};

type BenchmarkScenario = {
  readonly name: string;
  readonly schema: unknown;
  readonly seed: (db: BenchmarkDatabase) => void;
  readonly query: (builder: unknown) => unknown;
  readonly transformAST?: ((ast: unknown) => unknown) | undefined;
};

type ScenarioEntry = {
  readonly scenario: BenchmarkScenario;
  readonly sourceSlug: string | undefined;
};

type QuerySeen = {
  readonly table: string;
  readonly sql: string;
  calls: number;
};

type BenchmarkRun = BenchmarkSuccess | BenchmarkFailure;

type BenchmarkSuccess = {
  readonly status: 'ok';
  readonly scenarioName: string;
  readonly rowCount: number;
  readonly rowDigest: string;
  readonly medianMs: number;
  readonly minMs: number;
  readonly maxMs: number;
  readonly totalCalls: number;
  readonly uniqueSQL: number;
  readonly sql: readonly QuerySeen[];
  readonly planDebug: string;
};

type BenchmarkFailure = {
  readonly status: 'error';
  readonly scenarioName: string;
  readonly error: string;
};

type Target = {
  readonly label: string;
  readonly root: string;
  readonly commit: string | undefined;
};

type Comparison = {
  readonly context: string;
  readonly left: BenchmarkRun;
  readonly right: BenchmarkRun;
  readonly sameRows: boolean;
  readonly msRatio: number | undefined;
  readonly callRatio: number | undefined;
};

type BenchmarkDatabase = {
  exec(sql: string): void;
  prepare(sql: string): {run(...values: readonly unknown[]): void};
  [Symbol.dispose](): void;
};

type DebugInstance = {
  initQuery(table: string, query: string): void;
};

type RuntimeModules = {
  readonly testLogConfig: unknown;
  readonly createSilentLogContext: () => unknown;
  readonly computeZqlSpecs: (
    lc: unknown,
    db: BenchmarkDatabase,
    options: {readonly includeBackfillingColumns: false},
    tableSpecs: Map<string, unknown>,
  ) => void;
  readonly createTableMetadataTable: string;
  readonly buildPipeline: (
    ast: unknown,
    delegate: BenchmarkDelegate,
    queryID: string,
    costModel: unknown,
    lc: unknown,
    planDebugger: PlanDebugger,
  ) => unknown;
  readonly Debug: new () => DebugInstance;
  readonly Catch: new (input: unknown) => {
    fetch(): readonly unknown[];
    destroy(): void;
  };
  readonly createBuilder: (schema: unknown) => unknown;
  readonly asQueryInternals: (query: unknown) => {readonly ast: unknown};
  readonly Database: new (lc: unknown, path: string) => BenchmarkDatabase;
  readonly createSQLiteCostModel: (
    db: BenchmarkDatabase,
    tableSpecs: Map<string, unknown>,
  ) => unknown;
  readonly newQueryDelegate: (
    lc: unknown,
    logConfig: unknown,
    db: BenchmarkDatabase,
    schema: unknown,
  ) => BenchmarkDelegate;
  readonly AccumulatorDebugger: new () => PlanDebugger;
};

type BenchmarkDelegate = {
  debug?: DebugInstance | undefined;
};

type PlanDebugger = {
  format(): string;
};

const DEFAULT_SCENARIO_SOURCE =
  'packages/zqlite/src/test/query-scenarios/scenarios/index.ts';
const DEFAULT_ITERATIONS = 9;
const DEFAULT_WARMUPS = 2;

const options = parseArgs(process.argv.slice(2));
const repoRoot = git(['rev-parse', '--show-toplevel'], process.cwd());

const scenarioCatalog = await loadScenarios(
  resolveInputPath(options.scenarioSource),
);
const leftScenarios = selectScenarios(scenarioCatalog, {
  scenario: options.leftScenario ?? options.scenario,
  side: 'left',
});
const rightScenarios = selectScenarios(scenarioCatalog, {
  scenario: options.rightScenario ?? options.scenario,
  side: 'right',
});

if (leftScenarios.length !== rightScenarios.length) {
  throw new Error(
    `Scenario count mismatch: left selected ${leftScenarios.length}, right selected ${rightScenarios.length}`,
  );
}

const [leftTarget, rightTarget] = await Promise.all([
  resolveTarget({label: 'left', value: options.left}),
  resolveTarget({label: 'right', value: options.right}),
]);
const [leftModules, rightModules] = await Promise.all([
  loadRuntimeModules(leftTarget.root),
  loadRuntimeModules(rightTarget.root),
]);

const comparisons: Comparison[] = [];
for (let index = 0; index < leftScenarios.length; index++) {
  const leftScenario = leftScenarios[index].scenario;
  const rightScenario = rightScenarios[index].scenario;
  const [leftRun, rightRun] = await Promise.all([
    runScenario(leftModules, leftScenario, options),
    runScenario(rightModules, rightScenario, options),
  ]);
  comparisons.push({
    context:
      leftScenario.name === rightScenario.name
        ? leftScenario.name
        : `${leftScenario.name} -> ${rightScenario.name}`,
    left: leftRun,
    right: rightRun,
    sameRows:
      leftRun.status === 'ok' &&
      rightRun.status === 'ok' &&
      leftRun.rowDigest === rightRun.rowDigest,
    msRatio:
      leftRun.status !== 'ok' ||
      rightRun.status !== 'ok' ||
      rightRun.medianMs === 0
        ? undefined
        : leftRun.medianMs / rightRun.medianMs,
    callRatio:
      leftRun.status !== 'ok' ||
      rightRun.status !== 'ok' ||
      rightRun.totalCalls === 0
        ? undefined
        : leftRun.totalCalls / rightRun.totalCalls,
  });
}

const output = {
  left: leftTarget,
  right: rightTarget,
  iterations: options.iterations,
  warmups: options.warmups,
  comparisons,
};

if (options.format === 'json') {
  print(`${JSON.stringify(output, null, 2)}\n`);
} else {
  print(`${formatMarkdown(output)}\n`);
}

function parseArgs(args: readonly string[]): Options {
  let left = 'origin/main';
  let right = '.';
  let scenarioSource = DEFAULT_SCENARIO_SOURCE;
  let scenario: string | undefined;
  let leftScenario: string | undefined;
  let rightScenario: string | undefined;
  let iterations = DEFAULT_ITERATIONS;
  let warmups = DEFAULT_WARMUPS;
  let format: Format = 'markdown';

  for (let index = 0; index < args.length; index++) {
    const arg = args[index];
    const next = () => {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error(`Missing value for ${arg}`);
      }
      index++;
      return value;
    };

    switch (arg) {
      case '--left':
        left = next();
        break;
      case '--right':
        right = next();
        break;
      case '--scenario-source':
        scenarioSource = next();
        break;
      case '--scenario':
        scenario = next();
        break;
      case '--left-scenario':
        leftScenario = next();
        break;
      case '--right-scenario':
        rightScenario = next();
        break;
      case '--iterations':
        iterations = parsePositiveInteger(next(), arg);
        break;
      case '--warmups':
        warmups = parseNonNegativeInteger(next(), arg);
        break;
      case '--format': {
        const value = next();
        if (value !== 'json' && value !== 'markdown') {
          throw new Error('--format must be json or markdown');
        }
        format = value;
        break;
      }
      case '--help':
        printHelp();
        process.exit(0);
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  return {
    left,
    right,
    scenarioSource,
    scenario,
    leftScenario,
    rightScenario,
    iterations,
    warmups,
    format,
  };
}

async function resolveTarget({
  label,
  value,
}: {
  readonly label: string;
  readonly value: string;
}): Promise<Target> {
  const path = resolveInputPath(value);
  if (existsSync(path)) {
    return {label: value, root: realpathSync(path), commit: undefined};
  }

  const commit = git(['rev-parse', '--verify', `${value}^{commit}`]);
  const shortCommit = commit.slice(0, 12);
  const worktree = resolve(
    repoRoot,
    '.tmp',
    'query-scenario-worktrees',
    `${label}-${safeName(value)}-${shortCommit}`,
  );
  if (!existsSync(worktree)) {
    await mkdir(resolve(repoRoot, '.tmp', 'query-scenario-worktrees'), {
      recursive: true,
    });
    git(['worktree', 'add', '--detach', worktree, commit]);
  }
  return {label: value, root: worktree, commit};
}

async function loadScenarios(
  source: string,
): Promise<readonly ScenarioEntry[]> {
  const module = (await import(pathToFileURL(source).href)) as {
    readonly default: BenchmarkScenario | readonly BenchmarkScenario[];
  };
  const scenarios = Array.isArray(module.default)
    ? module.default
    : [module.default as BenchmarkScenario];
  const sourceSlugByScenario =
    basename(source) === 'index.ts'
      ? await loadScenarioFileSlugs(dirname(source))
      : mapScenariosToSlug(
          scenarios,
          slugifyScenarioName(basename(source, '.ts')),
        );

  return scenarios.map(scenario => ({
    scenario,
    sourceSlug: sourceSlugByScenario.get(scenario),
  }));
}

async function loadScenarioFileSlugs(
  directory: string,
): Promise<Map<BenchmarkScenario, string>> {
  const slugByScenario = new Map<BenchmarkScenario, string>();
  const entries = await readdir(directory);
  for (const entry of entries) {
    if (!entry.endsWith('.ts') || entry === 'index.ts') {
      continue;
    }
    const module = (await import(
      pathToFileURL(resolve(directory, entry)).href
    )) as {
      readonly default: BenchmarkScenario | readonly BenchmarkScenario[];
    };
    const scenarios = Array.isArray(module.default)
      ? module.default
      : [module.default as BenchmarkScenario];
    for (const scenario of scenarios) {
      slugByScenario.set(scenario, slugifyScenarioName(basename(entry, '.ts')));
    }
  }
  return slugByScenario;
}

function mapScenariosToSlug(
  scenarios: readonly BenchmarkScenario[],
  sourceSlug: string,
): Map<BenchmarkScenario, string> {
  const slugByScenario = new Map<BenchmarkScenario, string>();
  for (const scenario of scenarios) {
    slugByScenario.set(scenario, sourceSlug);
  }
  return slugByScenario;
}

function selectScenarios(
  scenarios: readonly ScenarioEntry[],
  {
    scenario,
    side,
  }: {readonly scenario: string | undefined; readonly side: string},
): readonly ScenarioEntry[] {
  if (scenario === undefined || scenario === 'all') {
    return scenarios;
  }

  const normalized = scenario.endsWith('.ts')
    ? basename(scenario, '.ts')
    : scenario;
  const normalizedSlug = slugifyScenarioName(normalized);
  const selected = scenarios.filter(entry => {
    const nameSlug = slugifyScenarioName(entry.scenario.name);
    const fileSlug = entry.sourceSlug;
    return (
      entry.scenario.name === scenario ||
      nameSlug === normalizedSlug ||
      nameSlug.includes(normalizedSlug) ||
      normalizedSlug.includes(nameSlug) ||
      fileSlug === normalizedSlug
    );
  });
  if (selected.length === 0) {
    throw new Error(`No ${side} scenario matched ${scenario}`);
  }
  if (selected.length > 1) {
    throw new Error(
      `${side} scenario ${scenario} matched more than one scenario: ${selected
        .map(candidate => candidate.scenario.name)
        .join(', ')}`,
    );
  }
  return selected;
}

async function loadRuntimeModules(root: string): Promise<RuntimeModules> {
  const [
    otel,
    logging,
    liteTables,
    metadata,
    builder,
    debugDelegate,
    catchModule,
    createBuilderModule,
    queryInternals,
    dbModule,
    costModelModule,
    sourceFactory,
    plannerDebug,
  ] = await Promise.all([
    importFrom(root, 'packages/otel/src/test-log-config.ts'),
    importFrom(root, 'packages/shared/src/logging-test-utils.ts'),
    importFrom(root, 'packages/zero-cache/src/db/lite-tables.ts'),
    importFrom(
      root,
      'packages/zero-cache/src/services/replicator/schema/table-metadata.ts',
    ),
    importFrom(root, 'packages/zql/src/builder/builder.ts'),
    importFrom(root, 'packages/zql/src/builder/debug-delegate.ts'),
    importFrom(root, 'packages/zql/src/ivm/catch.ts'),
    importFrom(root, 'packages/zql/src/query/create-builder.ts'),
    importFrom(root, 'packages/zql/src/query/query-internals.ts'),
    importFrom(root, 'packages/zqlite/src/db.ts'),
    importFrom(root, 'packages/zqlite/src/sqlite-cost-model.ts'),
    importFrom(root, 'packages/zqlite/src/test/source-factory.ts'),
    importFrom(root, 'packages/zql/src/planner/planner-debug.ts'),
  ]);

  return {
    testLogConfig: property(otel, 'testLogConfig'),
    createSilentLogContext: property(logging, 'createSilentLogContext'),
    computeZqlSpecs: property(liteTables, 'computeZqlSpecs'),
    createTableMetadataTable: property(metadata, 'CREATE_TABLE_METADATA_TABLE'),
    buildPipeline: property(builder, 'buildPipeline'),
    Debug: property(debugDelegate, 'Debug'),
    Catch: property(catchModule, 'Catch'),
    createBuilder: property(createBuilderModule, 'createBuilder'),
    asQueryInternals: property(queryInternals, 'asQueryInternals'),
    Database: property(dbModule, 'Database'),
    createSQLiteCostModel: property(costModelModule, 'createSQLiteCostModel'),
    newQueryDelegate: property(sourceFactory, 'newQueryDelegate'),
    AccumulatorDebugger: property(plannerDebug, 'AccumulatorDebugger'),
  } as RuntimeModules;
}

function runScenario(
  modules: RuntimeModules,
  scenario: BenchmarkScenario,
  options: Options,
): BenchmarkRun {
  const lc = modules.createSilentLogContext();
  const db = new modules.Database(lc, ':memory:');
  try {
    scenario.seed(db);
    db.exec(modules.createTableMetadataTable);
    db.exec('ANALYZE');

    const tableSpecs = new Map<string, unknown>();
    modules.computeZqlSpecs(
      lc,
      db,
      {includeBackfillingColumns: false},
      tableSpecs,
    );
    const costModel = modules.createSQLiteCostModel(db, tableSpecs);
    const builder = modules.createBuilder(scenario.schema);
    const queryAST = modules.asQueryInternals(scenario.query(builder)).ast;
    const ast = scenario.transformAST
      ? scenario.transformAST(queryAST)
      : queryAST;
    const debug = makeCountingDebug(modules.Debug);
    const samples: number[] = [];
    let rowCount = 0;
    let rowDigest = '';
    let planDebug = '';

    for (
      let iteration = 0;
      iteration < options.warmups + options.iterations;
      iteration++
    ) {
      debug.reset();
      const delegate = modules.newQueryDelegate(
        lc,
        modules.testLogConfig,
        db,
        scenario.schema,
      );
      delegate.debug = debug.instance;
      const planDebugger = new modules.AccumulatorDebugger();
      const start = performance.now();
      const input = modules.buildPipeline(
        ast,
        delegate,
        'query-scenario-compare',
        costModel,
        lc,
        planDebugger,
      );
      const sink = new modules.Catch(input);
      const rows = sink
        .fetch()
        .filter(node => node !== 'yield')
        .map(node => (node as {readonly row: unknown}).row);
      sink.destroy();
      const elapsed = performance.now() - start;
      rowCount = rows.length;
      rowDigest = stableStringify(rows);
      if (iteration >= options.warmups) {
        samples.push(elapsed);
      }
      planDebug = planDebugger.format();
    }

    samples.sort((a, b) => a - b);
    const maxMs = samples.at(-1);
    if (maxMs === undefined) {
      throw new Error('No measured benchmark samples were captured');
    }
    return {
      status: 'ok',
      scenarioName: scenario.name,
      rowCount,
      rowDigest,
      medianMs: round(samples[Math.floor(samples.length / 2)]),
      minMs: round(samples[0]),
      maxMs: round(maxMs),
      totalCalls: debug.queries.reduce(
        (total, query) => total + query.calls,
        0,
      ),
      uniqueSQL: debug.queries.length,
      sql: debug.queries,
      planDebug,
    };
  } catch (error) {
    return {
      status: 'error',
      scenarioName: scenario.name,
      error: errorMessage(error),
    };
  } finally {
    db[Symbol.dispose]();
  }
}

function makeCountingDebug(DebugClass: new () => DebugInstance): {
  readonly instance: DebugInstance;
  readonly queries: readonly QuerySeen[];
  reset(): void;
} {
  let queries: QuerySeen[] = [];
  class CountingDebug extends DebugClass {
    override initQuery(table: string, query: string): void {
      const seen = queries.find(
        candidate => candidate.table === table && candidate.sql === query,
      );
      if (seen) {
        seen.calls++;
      } else {
        queries.push({table, sql: query, calls: 1});
      }
      super.initQuery(table, query);
    }
  }
  return {
    instance: new CountingDebug(),
    get queries() {
      return queries;
    },
    reset() {
      queries = [];
    },
  };
}

function formatMarkdown({
  left,
  right,
  iterations,
  warmups,
  comparisons,
}: {
  readonly left: Target;
  readonly right: Target;
  readonly iterations: number;
  readonly warmups: number;
  readonly comparisons: readonly Comparison[];
}): string {
  const lines = [
    `Compared \`${left.label}\` to \`${right.label}\` with ${iterations} measured iterations after ${warmups} warmups.`,
    '',
    '| Context | Before | After |',
    '| --- | --- | --- |',
  ];

  for (const comparison of comparisons) {
    lines.push(
      `| ${escapeMarkdown(comparison.context)} | ${formatRun(comparison.left)} | ${formatRun(
        comparison.right,
      )} |`,
    );
  }

  if (
    comparisons.some(
      comparison =>
        comparison.left.status === 'ok' &&
        comparison.right.status === 'ok' &&
        !comparison.sameRows,
    )
  ) {
    lines.push('');
    lines.push(
      'Warning: at least one comparison returned different rows. That can be expected when comparing two different queries, but it is a correctness red flag when comparing the same query across commits.',
    );
  }
  if (
    comparisons.some(
      comparison =>
        comparison.left.status === 'error' ||
        comparison.right.status === 'error',
    )
  ) {
    lines.push('');
    lines.push(
      'Warning: at least one comparison hit an error. That usually means the scenario is documenting an unsupported or not-yet-plannable shape.',
    );
  }

  return lines.join('\n');
}

function formatRun(run: BenchmarkRun): string {
  if (run.status === 'error') {
    return `error: ${escapeMarkdown(run.error)}`;
  }
  return `${formatMs(run.medianMs)}, ${run.totalCalls.toLocaleString()} SQL calls, ${run.uniqueSQL} SQL shapes, ${run.rowCount.toLocaleString()} rows`;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function resolveInputPath(value: string): string {
  return isAbsolute(value) ? value : resolve(repoRoot, value);
}

function importFrom(root: string, relativePath: string): Promise<unknown> {
  return import(pathToFileURL(resolve(root, relativePath)).href);
}

function property<T>(module: unknown, name: string): T {
  if (module === null || typeof module !== 'object' || !(name in module)) {
    throw new Error(`Imported module did not export ${name}`);
  }
  return module[name as keyof typeof module] as T;
}

function git(args: readonly string[], cwd = repoRoot): string {
  const result = spawnSync('git', args, {
    cwd,
    encoding: 'utf8',
  });
  if (result.status !== 0) {
    throw new Error(
      `git ${args.join(' ')} failed:\n${result.stderr || result.stdout}`,
    );
  }
  return result.stdout.trim();
}

function slugifyScenarioName(name: string): string {
  return name
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, '-')
    .replaceAll(/^-|-$/g, '');
}

function safeName(value: string): string {
  return value.replaceAll(/[^a-zA-Z0-9._-]+/g, '-').slice(0, 80);
}

function parsePositiveInteger(value: string, label: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }
  return parsed;
}

function parseNonNegativeInteger(value: string, label: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${label} must be a non-negative integer`);
  }
  return parsed;
}

function round(value: number): number {
  return Math.round(value * 10) / 10;
}

function formatMs(value: number): string {
  return `${value.toFixed(1)} ms`;
}

function escapeMarkdown(value: string): string {
  return value.replaceAll('|', '\\|');
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    return `{${Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry)}`)
      .join(',')}}`;
  }
  return JSON.stringify(value) ?? 'undefined';
}

function printHelp(): void {
  print(`Compare ZQL scenario query performance across local Zero commits.

Usage:
  npm --workspace=zql-benchmarks run bench:scenario -- [options]

Options:
  --left <ref-or-path>          Left side. Defaults to origin/main.
  --right <ref-or-path>         Right side. Defaults to current directory.
  --scenario <name-or-file>     Run one scenario on both sides. Defaults to all.
  --left-scenario <name-or-file>
  --right-scenario <name-or-file>
                               Compare two different scenario queries.
  --scenario-source <path>      Module exporting one scenario or an array.
                               Defaults to zqlite query-scenarios index.
  --iterations <n>              Measured iterations. Defaults to 9.
  --warmups <n>                 Warmup iterations. Defaults to 2.
  --format markdown|json        Defaults to markdown.
  --help                        Show this help.

Examples:
  Compare this checkout against main for one scenario:
    npm --workspace=zql-benchmarks run bench:scenario -- --left origin/main --right . --scenario student-membership-mixed-or

  Compare two local commits:
    npm --workspace=zql-benchmarks run bench:scenario -- --left 9587e11 --right 04dc506 --scenario permission-and-class-filter-intersection

  Compare two different query shapes on the same checkout:
    npm --workspace=zql-benchmarks run bench:scenario -- --left . --right . --left-scenario parent-or-exists-union-roots --right-scenario student-membership-mixed-or

  Capture SQL shapes and planner debug text:
    npm --workspace=zql-benchmarks run bench:scenario -- --left origin/main --right . --format json > .tmp/query-scenario-compare.json

Scenario selection accepts the full scenario name, "all", or a filename-ish slug
like "student-membership-mixed-or".

Refs are resolved locally with git rev-parse. The script creates detached
worktrees under .tmp/query-scenario-worktrees and never fetches from the network.
Use --right . when you want to benchmark the current working tree.`);
}

function print(value: string): void {
  process.stdout.write(value);
}
