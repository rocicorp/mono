import type {LogContext} from '@rocicorp/logger';
import auth from 'basic-auth';
import type {FastifyReply, FastifyRequest} from 'fastify';
import {Database} from '../../../zqlite/src/db.ts';
import {
  decodeSampleKinds,
  type SampleValueKind,
} from '../../../zqlite/src/sqlite-stat4-sample.ts';
import type {NormalizedZeroConfig as ZeroConfig} from '../config/normalize.ts';
import {getServerVersion, isAdminPasswordValid} from '../config/zero-config.ts';
import {
  computeZqlSpecsFromLiteSpecs,
  listIndexes,
  listTables,
} from '../db/lite-tables.ts';
import {StatementRunner} from '../db/statements.ts';
import {getReplicationState} from './replicator/schema/replication-state.ts';

/**
 * How the statistics in the replica were gathered. zero-cache only ever runs
 * `PRAGMA optimize`, and always under an `analysis_limit`, which samples rather
 * than scanning. A replica that someone ran `ANALYZE` on by hand has better
 * statistics, which is visible in `sqlite_stat4` having rows at all.
 */
const APPROXIMATE_METHOD =
  'PRAGMA optimize under an analysis_limit. Row counts and per-key averages ' +
  'are sampled estimates, and sqlite_stat4 is not collected.';
const FULL_METHOD =
  'ANALYZE, with no analysis limit. sqlite_stat4 has rows, so a full ANALYZE ' +
  'ran against this replica at some point.';

/**
 * A fixed glossary, sent with every bundle. The audience is a language model
 * that has the user's codebase but has never seen these statistics, and would
 * otherwise read a sampled estimate as an exact count.
 */
const ABOUT = {
  whatThisIs:
    'Schema, indexes and SQLite statistics for the SQLite replica that ' +
    'zero-cache queries. Use it to decide how to rewrite ZQL queries and ' +
    'which indexes to add. It contains no row data.',
  stat1Format:
    'sqlite_stat1.stat is "<rows> <avg rows per distinct 1-column prefix> ' +
    '<... per 2-column prefix> ...", parsed here into `rows` and ' +
    '`avgRowsPerPrefix`. NULLs count as ordinary values, so a sparse column ' +
    'looks less selective than it is. Trailing flags such as "unordered", ' +
    '"noskipscan" and "sz=N" are kept in `flags`.',
  stat4Format:
    'sqlite_stat4 is a per-index histogram. For each sample, `nEq` is the ' +
    'rows equal to it, `nLt` the rows less than it, and `nDLt` the distinct ' +
    'values less than it, one entry per key column. A large `nEq` next to a ' +
    'small `nDLt` means a skewed column, where the average in stat1 hides ' +
    'the common values. Sample values are never returned: `sample` reports ' +
    'only each key column’s type.',
  howThePlannerUsesThis: [
    'Cost comes from SQLite scanstatus estimates for the SQL each source ' +
      'connection would run, so the statistics here are what the planner sees.',
    'Join fanout comes from sqlite_stat4, else sqlite_stat1, else a default ' +
      'of 3. With no stat4 (the usual case) fanout on a nullable foreign key ' +
      'is overestimated, because stat1 counts NULLs.',
    'The planner chooses per join whether to run it as a semi-join or to flip ' +
      'it so the child drives. An index that matches the child’s ' +
      'constraint plus ordering is what makes a flip cheap.',
  ],
  whatYouCanChange: [
    'The ZQL query: `whereExists` vs `related`, the order of conditions, and ' +
      'ordering that matches an existing index.',
    'A `limit`, which lets the planner stop early.',
    'Indexes: add them in Postgres, not on the replica. The replica copies ' +
      'upstream indexes, so a new index reaches it through replication.',
    'The planner flags in `server.planner`.',
  ],
  caveats: [
    'Row counts are estimates from the statistics, not COUNT(*). This ' +
      'endpoint never scans a table.',
    'Statistics are as of the last time they were gathered, which SQLite does ' +
      'not record. A table whose size changed a lot since then can be far off.',
    'Tables in `tables` with `syncable: false` cannot be queried by clients: ' +
      'they lack a primary key or unique index, or every column has a type ' +
      'ZQL does not support.',
  ],
  nextStep:
    'POST an AST to /plannerz/analyze to see the plan the planner picks for ' +
    'one query, with the SQLite query plans it generates.',
} as const;

export type Stat1 = {
  /** The raw `sqlite_stat1.stat` text, in case a flag is not parsed here. */
  raw: string;
  /** Estimated rows in the table. */
  rows: number;
  /**
   * Average rows per distinct key prefix: the first entry is for a 1-column
   * prefix, the second for 2 columns, and so on.
   */
  avgRowsPerPrefix: number[];
  /** Non-numeric tokens, e.g. `unordered`, `noskipscan`, `sz=N`. */
  flags: string[];
};

/** A `sqlite_stat4` sample with its values replaced by their types. */
export type Stat4Sample = {
  nEq: number[];
  nLt: number[];
  nDLt: number[];
  /** One entry per key column, plus the trailing rowid. */
  sample: SampleValueKind[];
};

/**
 * A digest of an index's `sqlite_stat4` histogram, one entry per key prefix
 * depth. This is what the default response carries: a full histogram runs to
 * hundreds of samples per index, which is a lot of context to spend on data an
 * LLM would only summarize anyway.
 */
export type Stat4PrefixSummary = {
  /** Rows equal to the most common sampled key at this depth. */
  maxRowsPerKey: number;
  /** Median rows per sampled key at this depth. */
  medianRowsPerKey: number;
  /**
   * Lower bound on distinct keys at this depth, from the last sample's count of
   * distinct values less than it.
   */
  estimatedDistinctKeys: number;
};

export type Stat4Summary = {
  samples: number;
  /** The type of each key column, and of the trailing rowid. */
  sampleKinds: SampleValueKind[];
  /** One entry per key prefix depth: 1 column, then 2, and so on. */
  perPrefix: Stat4PrefixSummary[];
};

export type IndexStats = {
  name: string;
  columns: {name: string; dir: 'ASC' | 'DESC'}[];
  unique: boolean;
  partial: boolean;
  stat1?: Stat1 | undefined;
  stat4?: Stat4Summary | undefined;
  /** The full histogram, only with `?stat4=full`. */
  stat4Samples?: Stat4Sample[] | undefined;
};

export type ColumnStats = {
  name: string;
  dataType: string;
  nullable: boolean;
  /** The ZQL type, or `null` for a column clients cannot see. */
  zqlType: string | null;
  backfilling?: true | undefined;
};

export type TableStats = {
  name: string;
  columns: ColumnStats[];
  primaryKey: string[] | undefined;
  estimatedRows: number | undefined;
  syncable: boolean;
  indexes: IndexStats[];
};

export type PlannerzBundle = {
  about: typeof ABOUT;
  server: {
    zeroVersion: string;
    sqliteVersion: string;
    replicaWatermark: string | undefined;
    generatedAt: string;
    planner: {
      enableQueryPlanner: boolean;
      enableCorrelatedPredicatePushdown: boolean;
      enablePlannerAwarePushdown: boolean;
    };
  };
  statsQuality: {
    stat1Present: boolean;
    stat4Rows: number;
    method: string;
    tablesWithoutStats: string[];
  };
  tables: TableStats[];
};

/**
 * Parses a `sqlite_stat1.stat` value.
 *
 * @visibleForTesting
 */
export function parseStat1(raw: string): Stat1 {
  const tokens = raw.trim().split(SEPARATOR).filter(Boolean);
  const numbers: number[] = [];
  const flags: string[] = [];
  for (const token of tokens) {
    // Only leading tokens are counts; anything else is a flag, even if it
    // parses as a number (e.g. a future "sz=N" spelled differently).
    if (flags.length === 0 && COUNT.test(token)) {
      numbers.push(parseInt(token, 10));
    } else {
      flags.push(token);
    }
  }
  return {
    raw,
    rows: numbers[0] ?? 0,
    avgRowsPerPrefix: numbers.slice(1),
    flags,
  };
}

/** Whitespace between the counts in a `sqlite_stat1` or `sqlite_stat4` value. */
const SEPARATOR = /\s+/;
/** A count, as opposed to a trailing flag such as `unordered` or `sz=N`. */
const COUNT = /^\d+$/;

type Stat1Row = {tbl: string; idx: string | null; stat: string};
type Stat4Row = {
  tbl: string;
  idx: string;
  neq: string;
  nlt: string;
  ndlt: string;
  sample: Buffer;
};

function tableExists(db: Database, name: string): boolean {
  return (
    db
      .prepare(
        `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`,
      )
      .all(name).length > 0
  );
}

function parseCounts(raw: string): number[] {
  return raw
    .trim()
    .split(SEPARATOR)
    .filter(Boolean)
    .map(n => parseInt(n, 10));
}

/**
 * Builds the `/plannerz` bundle from the replica's catalog, `sqlite_stat*`
 * tables and the server config. No user table is read, so the cost does not
 * depend on how much data the replica holds.
 *
 * @visibleForTesting
 */
export function buildPlannerzBundle(
  lc: LogContext,
  config: ZeroConfig,
  opts: BundleOptions = {},
): PlannerzBundle {
  using db = new Database(lc, config.replica.file, {readonly: true});
  return buildPlannerzBundleFromDB(lc, db, config, opts);
}

export type BundleOptions = {
  /** Include the full `sqlite_stat4` histogram, not just the digest. */
  fullStat4?: boolean | undefined;
};

/** @visibleForTesting */
export function buildPlannerzBundleFromDB(
  lc: LogContext,
  db: Database,
  config: ZeroConfig,
  opts: BundleOptions = {},
): PlannerzBundle {
  const tables = listReplicaTables(lc, db);
  const indexes = listIndexes(db);
  const zqlSpecs = computeZqlSpecsFromLiteSpecs(
    tables,
    indexes,
    {includeBackfillingColumns: false},
    undefined,
    undefined,
    lc,
  );

  const stat1Present = tableExists(db, 'sqlite_stat1');
  const stat1ByTableAndIndex = new Map<string, Stat1>();
  const statsByTable = new Map<string, Stat1>();
  if (stat1Present) {
    for (const {tbl, idx, stat} of db
      .prepare(`SELECT tbl, idx, stat FROM sqlite_stat1`)
      .all() as Stat1Row[]) {
      const parsed = parseStat1(stat);
      if (idx !== null) {
        stat1ByTableAndIndex.set(`${tbl}.${idx}`, parsed);
      }
      // Every row for a table starts with the table's row count. A row with a
      // null `idx` is written for a table with no indexes, and is preferred
      // because it is about the table itself.
      if (idx === null || !statsByTable.has(tbl)) {
        statsByTable.set(tbl, parsed);
      }
    }
  }

  const stat4Present = tableExists(db, 'sqlite_stat4');
  const stat4ByTableAndIndex = new Map<string, Stat4Sample[]>();
  let stat4Rows = 0;
  if (stat4Present) {
    for (const {tbl, idx, neq, nlt, ndlt, sample} of db
      .prepare(`SELECT tbl, idx, neq, nlt, ndlt, sample FROM sqlite_stat4`)
      .all() as Stat4Row[]) {
      stat4Rows++;
      const samples = stat4ByTableAndIndex.get(`${tbl}.${idx}`) ?? [];
      samples.push({
        nEq: parseCounts(neq),
        nLt: parseCounts(nlt),
        nDLt: parseCounts(ndlt),
        // Values are deliberately dropped here: only the types are reported.
        sample: decodeSampleKinds(sample),
      });
      stat4ByTableAndIndex.set(`${tbl}.${idx}`, samples);
    }
  }

  const indexesByTable = new Map<string, IndexStats[]>();
  for (const index of indexes) {
    const key = `${index.tableName}.${index.name}`;
    const stat4 = stat4ByTableAndIndex.get(key);
    const forTable = indexesByTable.get(index.tableName) ?? [];
    forTable.push({
      name: index.name,
      columns: Object.entries(index.columns).map(([name, dir]) => ({
        name,
        dir,
      })),
      unique: index.unique,
      partial: index.partial === true,
      stat1: stat1ByTableAndIndex.get(key),
      stat4: stat4?.length ? summarizeStat4(stat4) : undefined,
      stat4Samples: opts.fullStat4 && stat4?.length ? stat4 : undefined,
    });
    indexesByTable.set(index.tableName, forTable);
  }

  const tablesWithoutStats: string[] = [];
  const tableStats = tables.map(table => {
    const zqlSpec = zqlSpecs.get(table.name)?.zqlSpec;
    if (!statsByTable.has(table.name)) {
      tablesWithoutStats.push(table.name);
    }
    const backfilling = new Set(table.backfilling ?? []);
    return {
      name: table.name,
      columns: Object.entries(table.columns).map(([name, spec]) => ({
        name,
        dataType: spec.dataType,
        nullable: !spec.notNull,
        zqlType: zqlSpec?.[name]?.type ?? null,
        ...(backfilling.has(name) ? {backfilling: true as const} : {}),
      })),
      primaryKey: table.primaryKey ? [...table.primaryKey] : undefined,
      estimatedRows: statsByTable.get(table.name)?.rows,
      syncable: zqlSpec !== undefined,
      indexes: indexesByTable.get(table.name) ?? [],
    };
  });

  return {
    about: ABOUT,
    server: {
      zeroVersion: getServerVersion(config),
      sqliteVersion: sqliteVersion(db),
      replicaWatermark: replicaWatermark(lc, db),
      generatedAt: new Date().toISOString(),
      planner: {
        enableQueryPlanner: config.enableQueryPlanner,
        // Both default to on, matching services/analyze.ts.
        enableCorrelatedPredicatePushdown:
          config.enableCorrelatedPredicatePushdown !== false,
        enablePlannerAwarePushdown: config.enablePlannerAwarePushdown !== false,
      },
    },
    statsQuality: {
      stat1Present,
      stat4Rows,
      method: stat4Rows > 0 ? FULL_METHOD : APPROXIMATE_METHOD,
      tablesWithoutStats,
    },
    tables: tableStats,
  };
}

/**
 * Lists the replica's tables, tolerating a replica that has no metadata tables
 * yet. `listTables` reads `_zero.tableMetadata` for backfill status, which does
 * not exist until initial sync has created it. Reporting the schema without
 * backfill status beats failing the whole request.
 */
function listReplicaTables(lc: LogContext, db: Database) {
  try {
    return listTables(db);
  } catch (e) {
    lc.debug?.('reading table metadata failed; listing tables without it', e);
    return listTables(db, true, false);
  }
}

/**
 * Digests an index's stat4 samples.
 *
 * Note that `nLt` and `nDLt` are stored as text, so SQL cannot order the
 * samples by them numerically. This works off the whole set rather than
 * relying on the samples' order: the highest `nDLt` is a lower bound on the
 * distinct keys, since the sample holding it has that many distinct keys below
 * it plus itself.
 *
 * @visibleForTesting
 */
export function summarizeStat4(samples: Stat4Sample[]): Stat4Summary {
  const depth = Math.max(...samples.map(s => s.nEq.length));
  const perPrefix: Stat4PrefixSummary[] = [];
  for (let d = 0; d < depth; d++) {
    const counts = samples
      .map(s => s.nEq[d])
      .filter(n => n !== undefined)
      .sort((a, b) => a - b);
    const maxDistinctBelow = Math.max(
      ...samples.map(s => s.nDLt[d]).filter(n => n !== undefined),
    );
    perPrefix.push({
      maxRowsPerKey: counts.at(-1)!,
      medianRowsPerKey: counts[Math.floor((counts.length - 1) / 2)],
      estimatedDistinctKeys: maxDistinctBelow + 1,
    });
  }
  return {
    samples: samples.length,
    // Every sample of an index describes the same columns.
    sampleKinds: samples[0].sample,
    perPrefix,
  };
}

function sqliteVersion(db: Database): string {
  const [{version}] = db
    .prepare(`SELECT sqlite_version() AS version`)
    .all() as {version: string}[];
  return version;
}

function replicaWatermark(lc: LogContext, db: Database): string | undefined {
  try {
    return getReplicationState(new StatementRunner(db)).stateVersion;
  } catch (e) {
    // A replica that has not finished initial sync has no replication state.
    lc.debug?.('could not read replication state', e);
    return undefined;
  }
}

/**
 * Serves the replica's schema, indexes and SQLite statistics for consumption by
 * a language model, so that it can advise on query rewrites and indexes.
 *
 * Row data is never returned. See `designs/003_replica_stats_endpoint.md`.
 *
 * HTTP query parameters:
 * * `pretty`: indents the JSON
 * * `stat4=full`: includes the full sqlite_stat4 histogram, which is large
 */
export async function handlePlannerzRequest(
  lc: LogContext,
  config: ZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
) {
  const credentials = auth(req);
  if (!isAdminPasswordValid(lc, config, credentials?.pass)) {
    void res
      .code(401)
      .header('WWW-Authenticate', 'Basic realm="Plannerz Protected Area"')
      .send('Unauthorized');
    return;
  }

  const query = req.query as Record<string, unknown>;
  const indent = query.pretty !== undefined ? 2 : undefined;
  let bundle: PlannerzBundle;
  try {
    bundle = buildPlannerzBundle(lc, config, {
      fullStat4: query.stat4 === 'full',
    });
  } catch (e) {
    lc.error?.('error building plannerz bundle', e);
    await res.code(500).send({error: String(e)});
    return;
  }
  await res
    .header('Content-Type', 'application/json')
    .send(JSON.stringify(bundle, null, indent));
}
