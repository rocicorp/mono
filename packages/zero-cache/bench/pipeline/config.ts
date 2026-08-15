import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {PostgresDB} from '../../src/types/pg.ts';

export type SQLiteChangeLogMode = 'off' | 'write' | 'compare' | 'serve';

export interface LoadStats {
  readonly writesAttempted: number;
  readonly writesSucceeded: number;
  readonly writesFailed: number;
  readonly durationMs: number;
  readonly actualRate: number;
}

export type LoadGeneratorFn = (
  db: PostgresDB,
  ratePerSec: number,
  durationSec: number,
  signal: AbortSignal,
) => Promise<LoadStats>;

export type SeedDatabaseFn = (db: PostgresDB) => Promise<void>;

export interface BenchmarkConfig {
  // --- Topology ---
  /**
   * Number of replication-manager processes (1 or 2).
   * With 2 RMs, the benchmark runs the RMv2 HA configuration:
   * RM1 starts as active, RM2 starts as standby and takes ownership.
   */
  readonly numReplicationManagers: 1 | 2;

  /** Number of view-syncer processes (1 to N). */
  readonly numViewSyncers: number;

  /** Number of simulated WebSocket clients connected to each view-syncer. */
  readonly clientsPerViewSyncer: number;

  // --- Queries ---
  /** ASTs that simulated clients register. */
  readonly clientQueries: readonly AST[];

  // --- Load Generation ---
  /** Function determining what rows are written and at what rate. */
  readonly loadGenerator: LoadGeneratorFn;

  /** Target writes per second (0 = unthrottled / as fast as possible). */
  readonly writeRatePerSecond: number;

  /** Duration of the active load phase in seconds. */
  readonly loadDurationSeconds: number;

  /** Settling / drain timeout in seconds to wait for changes to propagate. */
  readonly drainTimeoutSeconds: number;

  // --- Seeding ---
  /** Function to seed initial data. Default: zbugs synthetic seed. */
  readonly seedDatabase: SeedDatabaseFn;

  // --- Database Setup ---
  /**
   * Database provisioning mode:
   * - 'docker': Uses docker-compose or containerized Postgres.
   * - 'external': Uses caller-supplied connection strings directly.
   */
  readonly dbMode: 'docker' | 'external';

  /** Upstream PostgreSQL connection string. */
  readonly upstreamDB?: string | undefined;

  /** Separate CVR PostgreSQL connection string. */
  readonly cvrDB?: string | undefined;

  /** Separate Change Log PostgreSQL connection string. */
  readonly changeDB?: string | undefined;

  // --- RMv2 & SQLite Change Log ---
  /** SQLite change-log mode for RM (default: 'serve'). */
  readonly sqliteChangeLogMode: SQLiteChangeLogMode;

  /** Shard / App ID for the benchmark run. */
  readonly appID: string;

  // --- Profiling ---
  /** Collect CPU profile from replication-manager process(es). */
  readonly profileReplicationManager: boolean;

  /** Collect CPU profile from view-syncer process(es). */
  readonly profileViewSyncer: boolean;

  // --- Output ---
  /** Output directory for benchmark reports, JSON metrics, and CPU profiles. */
  readonly outputDir: string;

  /** Log level for zero-cache workers ('error' | 'warn' | 'info' | 'debug'). */
  readonly logLevel: 'error' | 'warn' | 'info' | 'debug';
}
