import {existsSync, statSync} from 'node:fs';
import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import {Database} from '../../../zqlite/src/db.ts';

const SQLITE_CORRUPTION_CODES = new Set(['SQLITE_CORRUPT', 'SQLITE_NOTADB']);
const SQLITE_CORRUPTION_MESSAGE =
  /database disk image is malformed|file is not a database|database is malformed|malformed database/i;
const SQLITE_FILE_SUFFIXES = ['', '-wal', '-wal2', '-shm', '-journal'] as const;
const MAX_LOGGED_CHECK_ISSUES = 20;

type ErrorSummary = {
  readonly name: string;
  readonly message: string;
  readonly code?: string | undefined;
  readonly cause?: ErrorSummary | string | undefined;
};

type SQLiteFileStats = {
  readonly suffix: string;
  readonly path: string;
  readonly sizeBytes: number;
  readonly mtimeMs: number;
};

type CheckSummary = {
  readonly status: 'ok' | 'failed';
  readonly rowCount: number;
  readonly issueCount: number;
  readonly loggedIssueCount: number;
  readonly truncated: boolean;
  readonly issues: string[];
};

type DiagnosticTarget = {
  readonly debugName: string;
  readonly dbPath: string;
};

const diagnosticTargets = new Map<number, DiagnosticTarget>();
let nextDiagnosticTargetID = 0;

export function isSQLiteCorruption(e: unknown): boolean {
  return getSQLiteCorruptionCode(e) !== undefined;
}

export function registerSQLiteCorruptionDiagnosticTarget(
  target: DiagnosticTarget,
): () => void {
  const id = nextDiagnosticTargetID++;
  diagnosticTargets.set(id, target);
  return () => diagnosticTargets.delete(id);
}

export function logLastChanceSQLiteCorruptionDiagnostics(
  lc: LogContext,
  cause: unknown,
): boolean {
  if (!isSQLiteCorruption(cause) || diagnosticTargets.size === 0) {
    return false;
  }
  for (const {debugName, dbPath} of diagnosticTargets.values()) {
    logSQLiteCorruptionDiagnostics(lc, debugName, dbPath, cause);
  }
  return true;
}

export function logSQLiteCorruptionDiagnostics(
  lc: LogContext,
  debugName: string,
  dbPath: string,
  cause: unknown,
): void {
  const start = Date.now();
  lc.error?.('SQLite replica corruption detected', {
    debugName,
    dbPath,
    cause: summarizeError(cause),
  });

  logDiagnostic(lc, debugName, 'file-stats', () => ({
    files: getSQLiteFileStats(dbPath),
  }));

  let db: Database | undefined;
  try {
    db = new Database(lc, dbPath, {readonly: true});
    db.pragma('busy_timeout = 1000');
  } catch (e) {
    lc.error?.('SQLite corruption diagnostic failed', {
      debugName,
      step: 'open-readonly',
      durationMs: Date.now() - start,
      error: summarizeError(e),
    });
    return;
  }

  try {
    logDiagnostic(lc, debugName, 'basic-pragmas', () => ({
      pragmas: getBasicPragmas(db),
    }));
    logDiagnostic(lc, debugName, 'replica-metadata', () => ({
      metadata: getReplicaMetadata(db),
    }));
    logDiagnostic(lc, debugName, 'quick-check', () => ({
      check: runCheck(db, 'quick_check'),
    }));
    logDiagnostic(lc, debugName, 'integrity-check', () => ({
      check: runCheck(db, 'integrity_check'),
    }));
    lc.error?.('SQLite corruption diagnostics completed', {
      debugName,
      dbPath,
      durationMs: Date.now() - start,
    });
  } finally {
    db.close();
  }
}

function getSQLiteCorruptionCode(e: unknown): string | undefined {
  let current = e;
  const seen = new Set<unknown>();

  while (current !== undefined && current !== null && !seen.has(current)) {
    seen.add(current);

    const code = getErrorCode(current);
    if (code !== undefined && SQLITE_CORRUPTION_CODES.has(code)) {
      return code;
    }

    if (current instanceof SqliteError || current instanceof Error) {
      if (SQLITE_CORRUPTION_MESSAGE.test(current.message)) {
        return code ?? 'SQLITE_CORRUPT';
      }
      current = getErrorCause(current);
      continue;
    }

    break;
  }

  return undefined;
}

function getErrorCode(e: unknown): string | undefined {
  if (e && typeof e === 'object' && 'code' in e) {
    const code = (e as {readonly code?: unknown}).code;
    return typeof code === 'string' ? code : undefined;
  }
  return undefined;
}

function getErrorCause(e: Error): unknown {
  return (e as {readonly cause?: unknown}).cause;
}

function summarizeError(e: unknown, depth = 0): ErrorSummary | string {
  if (!(e instanceof Error)) {
    return String(e);
  }
  const cause = getErrorCause(e);
  return {
    name: e.name,
    message: e.message,
    code: getErrorCode(e),
    cause:
      cause === undefined || depth >= 4
        ? undefined
        : summarizeError(cause, depth + 1),
  };
}

function getSQLiteFileStats(dbPath: string): SQLiteFileStats[] {
  const stats: SQLiteFileStats[] = [];
  for (const suffix of SQLITE_FILE_SUFFIXES) {
    const path = `${dbPath}${suffix}`;
    if (!existsSync(path)) {
      continue;
    }
    const stat = statSync(path);
    stats.push({
      suffix,
      path,
      sizeBytes: stat.size,
      mtimeMs: stat.mtimeMs,
    });
  }
  return stats;
}

function getBasicPragmas(db: Database) {
  return {
    pageSize: firstPragmaValue<number>(db, 'page_size', 'page_size'),
    pageCount: firstPragmaValue<number>(db, 'page_count', 'page_count'),
    freelistCount: firstPragmaValue<number>(
      db,
      'freelist_count',
      'freelist_count',
    ),
    journalMode: firstPragmaValue<string>(db, 'journal_mode', 'journal_mode'),
    schemaVersion: firstPragmaValue<number>(
      db,
      'schema_version',
      'schema_version',
    ),
    userVersion: firstPragmaValue<number>(db, 'user_version', 'user_version'),
    applicationID: firstPragmaValue<number>(
      db,
      'application_id',
      'application_id',
    ),
  };
}

function getReplicaMetadata(db: Database) {
  return {
    versionHistory: queryDiagnostic(() =>
      db
        .prepare(
          `SELECT dataVersion, schemaVersion, minSafeVersion
           FROM "_zero.versionHistory"`,
        )
        .get(),
    ),
    subscriptionState: queryDiagnostic(() =>
      db
        .prepare(
          `SELECT c.replicaVersion, s.stateVersion as watermark
           FROM "_zero.replicationConfig" as c
           JOIN "_zero.replicationState" as s
           ON c.lock = s.lock`,
        )
        .get(),
    ),
    sqliteCatalogCounts: queryDiagnostic(() =>
      db
        .prepare(
          `SELECT type, count(*) as count
           FROM sqlite_master
           GROUP BY type
           ORDER BY type`,
        )
        .all(),
    ),
  };
}

function queryDiagnostic<T>(query: () => T):
  | {readonly ok: true; readonly value: T}
  | {
      readonly ok: false;
      readonly error: ErrorSummary | string;
    } {
  try {
    return {ok: true, value: query()};
  } catch (e) {
    return {ok: false, error: summarizeError(e)};
  }
}

function firstPragmaValue<T>(
  db: Database,
  pragma: string,
  column: string,
): T | undefined {
  const [row] = db.pragma<Record<string, T>>(pragma);
  return row?.[column];
}

function runCheck(db: Database, pragma: 'quick_check' | 'integrity_check') {
  const rows = db.pragma<Record<string, string>>(pragma);
  const issues = rows
    .map(row => row[pragma] ?? Object.values(row)[0] ?? '')
    .filter(issue => issue !== '' && issue !== 'ok');
  const loggedIssues = issues.slice(0, MAX_LOGGED_CHECK_ISSUES);
  return {
    status: issues.length === 0 ? 'ok' : 'failed',
    rowCount: rows.length,
    issueCount: issues.length,
    loggedIssueCount: loggedIssues.length,
    truncated: issues.length > loggedIssues.length,
    issues: loggedIssues,
  } satisfies CheckSummary;
}

function logDiagnostic<T>(
  lc: LogContext,
  debugName: string,
  step: string,
  fn: () => T,
): void {
  const start = Date.now();
  try {
    const result = fn();
    lc.error?.('SQLite corruption diagnostic', {
      debugName,
      step,
      durationMs: Date.now() - start,
      result,
    });
  } catch (e) {
    lc.error?.('SQLite corruption diagnostic failed', {
      debugName,
      step,
      durationMs: Date.now() - start,
      error: summarizeError(e),
    });
  }
}
