/* oxlint-disable no-console */
import {tmpdir} from 'node:os';
import {performance} from 'node:perf_hooks';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {deleteLiteDB} from '../../src/db/delete-lite-db.ts';
import {computeZqlSpecs, listTables} from '../../src/db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../src/db/specs.ts';
import type {DataOrSchemaChange} from '../../src/services/change-source/protocol/current/data.ts';
import {populateFromExistingTables} from '../../src/services/replicator/schema/column-metadata.ts';
import {initReplicationState} from '../../src/services/replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
} from '../../src/services/replicator/test-utils.ts';
import {Snapshotter} from '../../src/services/view-syncer/snapshotter.ts';
import type {Change} from '../../src/services/view-syncer/snapshotter.ts';
import type {RowValue} from '../../src/types/row-key.ts';
import {
  ACTIVE_TABLE,
  APP_ID,
  BACKGROUND_TABLE,
  INITIAL_VERSION,
  loadScenarios,
  watermark,
} from './fixtures.ts';
import {
  argValue,
  envInt,
  formatRate,
  percentile,
  writeJsonSummary,
} from './perf-utils.ts';
import type {
  DigestScenario,
  DigestStats,
  ScenarioSummary,
  Summary,
} from './types.ts';

const lc = createSilentLogContext();
const viewSyncerCount = envInt('ZERO_VS_DIGESTION_VIEW_SYNCERS', 16);
const activeTables = new Set([ACTIVE_TABLE]);
const messages = new ReplicationMessages({
  [ACTIVE_TABLE]: 'id',
  [BACKGROUND_TABLE]: 'id',
} as const);

const summaries: ScenarioSummary[] = [];
for (const scenario of loadScenarios()) {
  summaries.push(runScenario(scenario));
}

const summary: Summary = {
  name: 'zero-cache-view-syncer-digestion',
  generatedAt: new Date().toISOString(),
  viewSyncerCount,
  scenarios: summaries,
};

for (const result of summary.scenarios) {
  const pct = ((result.speedup - 1) * 100).toFixed(1);
  console.log(
    `${result.name}: old ${result.old.elapsedMs.toFixed(1)} ms | ` +
      `filtered ${result.filtered.elapsedMs.toFixed(1)} ms | ` +
      `${pct}% faster | ` +
      `materialized ${formatRate(result.old.materializedChanges)} -> ` +
      `${formatRate(result.filtered.materializedChanges)}`,
  );
}
console.log(JSON.stringify(summary));

await writeJsonSummary(
  summary,
  argValue('out') ?? process.env.ZERO_VS_DIGESTION_OUT,
);

function runScenario(scenario: DigestScenario): ScenarioSummary {
  const dbFile = `${tmpdir()}/zero-cache-vs-digestion-${process.pid}-${scenario.name}.db`;
  deleteLiteDB(dbFile);

  const db = new Database(lc, dbFile);
  const oldDigesters: Snapshotter[] = [];
  const filteredDigesters: Snapshotter[] = [];

  try {
    setupReplica(db);
    const tableSpecs = computeZqlSpecs(lc, db, {
      includeBackfillingColumns: false,
    });
    const allTableNames = new Set(tableSpecs.keys());

    for (let i = 0; i < viewSyncerCount; i++) {
      oldDigesters.push(createDigester(dbFile));
      filteredDigesters.push(createDigester(dbFile));
    }

    applyBacklog(db, scenario);

    const totalRows = scenario.transactions * scenario.rowsPerTransaction;
    const filtered = digest(
      filteredDigesters,
      tableSpecs,
      allTableNames,
      activeTables,
      true,
      totalRows,
    );
    const old = digest(
      oldDigesters,
      tableSpecs,
      allTableNames,
      activeTables,
      false,
      totalRows,
    );

    const activeRows =
      scenario.transactions * scenario.activeRowsPerTransaction;
    return {
      name: scenario.name,
      transactions: scenario.transactions,
      rowsPerTransaction: scenario.rowsPerTransaction,
      activeRowsPerTransaction: scenario.activeRowsPerTransaction,
      totalRows,
      activeRows,
      old,
      filtered,
      speedup:
        filtered.elapsedMs === 0
          ? Number.POSITIVE_INFINITY
          : old.elapsedMs / filtered.elapsedMs,
      elapsedDeltaMs: old.elapsedMs - filtered.elapsedMs,
      materializedChangeDelta:
        old.materializedChanges - filtered.materializedChanges,
    };
  } finally {
    for (const snapshotter of [...oldDigesters, ...filteredDigesters]) {
      snapshotter.destroy();
    }
    db.close();
    deleteLiteDB(dbFile);
  }
}

function createDigester(dbFile: string): Snapshotter {
  return new Snapshotter(lc, dbFile, {appID: APP_ID}).init();
}

function setupReplica(db: Database) {
  db.pragma('journal_mode = WAL2');
  db.exec(/*sql*/ `
    CREATE TABLE "${APP_ID}.permissions" (
      "lock"        INT PRIMARY KEY,
      "permissions" JSON,
      "hash"        TEXT,
      _0_version    TEXT NOT NULL
    );
    INSERT INTO "${APP_ID}.permissions" ("lock", "_0_version")
      VALUES (1, '${INITIAL_VERSION}');

    CREATE TABLE "${ACTIVE_TABLE}" (
      id TEXT PRIMARY KEY,
      value INTEGER,
      _0_version TEXT NOT NULL
    );
    CREATE TABLE "${BACKGROUND_TABLE}" (
      id TEXT PRIMARY KEY,
      value INTEGER,
      _0_version TEXT NOT NULL
    );
  `);
  initReplicationState(
    db,
    ['zero-cache-view-syncer-digestion'],
    INITIAL_VERSION,
  );
  populateFromExistingTables(db, listTables(db, false));
}

function applyBacklog(db: Database, scenario: DigestScenario) {
  const replicator = fakeReplicator(lc, db);
  let rowID = 0;
  for (let tx = 0; tx < scenario.transactions; tx++) {
    const changes: DataOrSchemaChange[] = [];
    for (let row = 0; row < scenario.rowsPerTransaction; row++) {
      const table =
        row < scenario.activeRowsPerTransaction
          ? ACTIVE_TABLE
          : BACKGROUND_TABLE;
      const value = tx * scenario.rowsPerTransaction + row;
      changes.push(insert(table, {id: `${table}-${rowID++}`, value}));
    }
    replicator.processTransaction(watermark(tx + 2), ...changes);
  }
}

function insert(
  table: typeof ACTIVE_TABLE | typeof BACKGROUND_TABLE,
  row: RowValue,
) {
  return table === ACTIVE_TABLE
    ? messages.insert(ACTIVE_TABLE, row)
    : messages.insert(BACKGROUND_TABLE, row);
}

function digest(
  digesters: readonly Snapshotter[],
  tableSpecs: Map<string, LiteAndZqlSpec>,
  allTableNames: Set<string>,
  activeTableNames: ReadonlySet<string>,
  filterInactiveTables: boolean,
  sourceChangeLogEntriesPerDigester: number,
): DigestStats {
  const timings: number[] = [];
  let selectedChangeLogEntries = 0;
  let materializedChanges = 0;
  let activeChanges = 0;
  const start = performance.now();

  for (const snapshotter of digesters) {
    const digesterStart = performance.now();
    const diff = snapshotter.advance(
      tableSpecs,
      allTableNames,
      filterInactiveTables ? activeTableNames : undefined,
    );
    selectedChangeLogEntries += diff.changes;
    for (const change of diff) {
      materializedChanges++;
      if (isActiveChange(change, activeTableNames)) {
        activeChanges++;
      }
    }
    timings.push(performance.now() - digesterStart);
  }

  const elapsedMs = performance.now() - start;
  const changeLogEntries = sourceChangeLogEntriesPerDigester * digesters.length;
  return {
    elapsedMs,
    changeLogEntries,
    selectedChangeLogEntries,
    materializedChanges,
    activeChanges,
    viewSyncerCount: digesters.length,
    p50ViewSyncerMs: percentile(timings, 50),
    p95ViewSyncerMs: percentile(timings, 95),
    rowsPerSecond: elapsedMs === 0 ? 0 : (changeLogEntries / elapsedMs) * 1000,
  };
}

function isActiveChange(
  change: Change,
  activeTableNames: ReadonlySet<string>,
): boolean {
  return activeTableNames.has(change.table);
}
