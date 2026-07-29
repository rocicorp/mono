/**
 * Property-based crash injection across the log-first commit ordering.
 *
 * The writer commits the change log before the replica, so the window between
 * the two commits is the one place where the two databases legitimately
 * disagree. A crash there must leave a *phantom* — a transaction in the log
 * whose replica commit was lost — and never a *hole*, because a phantom is
 * detectable and repairable at startup while a hole would be served to a
 * subscriber as a silent gap.
 *
 * Each generated case runs a change stream through {@link ChangeProcessor},
 * kills the process at a chosen point, then reopens both databases exactly as
 * `write-worker`'s `init` does and asserts design 5.1 invariants 1-4 plus
 * transaction-level contiguity. This is the writer-side companion to the
 * catchup handoff property test in
 * `change-streamer/sqlite-change-log-catchup.test.ts`.
 */

import fc from 'fast-check';
import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile, initDB} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../change-streamer/change-log-codec.ts';
import {
  CHANGE_LOG_META_TABLE,
  CHANGE_LOG_STREAM_TABLE,
  deleteChangeLogDB,
  openChangeLogDB,
  readReplicaAnchor,
  reconcileChangeLog,
  type ReconcileResult,
} from './change-log-db.ts';
import {ChangeProcessor} from './change-processor.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {ReplicationMessages} from './test-utils.ts';

const lc = createSilentLogContext();
const issues = new ReplicationMessages({issues: 'id'});

const REPLICA_VERSION = '02';

/** Where the process is killed, relative to the two commits. */
type CrashPoint =
  // The log's COMMIT never runs: both databases end at the previous version.
  | 'log-commit'
  // The log is committed; the replica's state update never runs.
  | 'state-update'
  // The log is committed and the state updated; the replica's COMMIT never runs.
  | 'replica-commit'
  // Not a crash: the source drops mid-transaction and the processor aborts.
  | 'source-disconnect';

type Scenario = {
  readonly before: number;
  readonly changes: number;
  readonly after: number;
  readonly crashPoint: CrashPoint;
};

const scenario: fc.Arbitrary<Scenario> = fc.record({
  before: fc.integer({min: 0, max: 4}),
  changes: fc.integer({min: 0, max: 3}),
  after: fc.integer({min: 0, max: 3}),
  crashPoint: fc.constantFrom<CrashPoint>(
    'log-commit',
    'state-update',
    'replica-commit',
    'source-disconnect',
  ),
});

/** Lexicographic watermarks, in order, starting after the replica version. */
function watermark(i: number): string {
  return `0${'3456789abcdefghijk'[i]}`;
}

class Crash extends Error {
  override readonly name = 'InjectedCrash';
}

/**
 * A replica runner that kills the process at a chosen statement, by closing
 * both connections before throwing. Closing rather than rolling back is what
 * makes this a crash: SQLite discards whatever was not committed, and the
 * processor's own rollback then fails against a closed handle, which is the
 * partial-rollback path `#fail` has to tolerate.
 */
class CrashingStatementRunner extends StatementRunner {
  kill: (() => void) | undefined;
  crashOnStateUpdate = false;
  crashOnCommit = false;

  override run(sql: string, ...args: unknown[]) {
    if (this.crashOnStateUpdate && sql.includes('"_zero.replicationState"')) {
      this.crashOnStateUpdate = false;
      this.#crash();
    }
    return super.run(sql, ...args);
  }

  override commit() {
    if (this.crashOnCommit) {
      this.crashOnCommit = false;
      this.#crash();
    }
    return super.commit();
  }

  #crash(): never {
    this.kill?.();
    throw new Crash('injected crash');
  }
}

/** A change-log runner that kills the process instead of committing. */
class CrashingChangeLogRunner extends StatementRunner {
  kill: (() => void) | undefined;
  crashOnCommit = false;

  override commit() {
    if (this.crashOnCommit) {
      this.crashOnCommit = false;
      this.kill?.();
      throw new Crash('injected crash');
    }
    return super.commit();
  }
}

type Session = {
  readonly replica: Database;
  readonly changeLog: Database;
  readonly replicaRunner: CrashingStatementRunner;
  readonly changeLogRunner: CrashingChangeLogRunner;
  readonly processor: ChangeProcessor;
  readonly failures: unknown[];
  readonly reconciled: ReconcileResult;
  readonly close: () => void;
};

/** Opens both databases and reconciles, exactly as `write-worker.init` does. */
function startSession(file: DbFile): Session {
  const replica = file.connect(lc);
  const changeLog = openChangeLogDB(lc, file.path, {readonly: false});
  changeLog.pragma('journal_mode = wal2');
  const reconciled = reconcileChangeLog(
    lc,
    changeLog,
    readReplicaAnchor(replica),
  );

  const replicaRunner = new CrashingStatementRunner(replica);
  const changeLogRunner = new CrashingChangeLogRunner(changeLog);
  const failures: unknown[] = [];
  let closed = false;
  const kill = () => {
    closed = true;
    changeLog.close();
    replica.close();
  };
  replicaRunner.kill = kill;
  changeLogRunner.kill = kill;

  return {
    replica,
    changeLog,
    replicaRunner,
    changeLogRunner,
    processor: new ChangeProcessor(
      replicaRunner,
      'serving',
      {changeLog: changeLogRunner},
      (_, err) => failures.push(err),
    ),
    failures,
    reconciled,
    close: () => {
      if (!closed) {
        closed = true;
        changeLog.close();
        replica.close();
      }
    },
  };
}

function process(session: Session, data: ChangeStreamData) {
  return session.processor.processMessage(
    lc,
    data,
    serializeChangeStreamData(data),
  );
}

/** A whole transaction: begin, `changes` inserts, commit. */
function transaction(session: Session, wm: string, changes: number): void {
  process(session, ['begin', issues.begin(), {commitWatermark: wm}]);
  for (let i = 0; i < changes; i++) {
    process(session, ['data', issues.insert('issues', {id: i, text: wm})]);
  }
  process(session, ['commit', issues.commit(), {watermark: wm}]);
}

type LogTransaction = {
  readonly watermark: string;
  readonly tags: readonly string[];
  readonly precommits: readonly (string | null)[];
  readonly positions: readonly number[];
};

function readLog(db: Database): LogTransaction[] {
  const rows = db
    .prepare(/*sql*/ `
      SELECT "watermark", "pos", json_extract("change", '$.tag') AS "tag",
             "precommit"
        FROM "${CHANGE_LOG_STREAM_TABLE}"
        ORDER BY "watermark", "pos"
    `)
    .all<{
      watermark: string;
      pos: number;
      tag: string;
      precommit: string | null;
    }>();

  const transactions: LogTransaction[] = [];
  for (const row of rows) {
    let last = transactions.at(-1);
    if (last?.watermark !== row.watermark) {
      last = {
        watermark: row.watermark,
        tags: [],
        precommits: [],
        positions: [],
      };
      transactions.push(last);
    }
    (last.tags as string[]).push(row.tag);
    (last.precommits as (string | null)[]).push(row.precommit);
    (last.positions as number[]).push(row.pos);
  }
  return transactions;
}

function head(db: Database): string | null {
  return db
    .prepare(
      /*sql*/ `SELECT max("watermark") AS "head" FROM "${CHANGE_LOG_STREAM_TABLE}"`,
    )
    .get<{head: string | null}>().head;
}

function stateVersion(db: Database): string {
  return db
    .prepare(/*sql*/ `SELECT "stateVersion" FROM "_zero.replicationState"`)
    .get<{stateVersion: string}>().stateVersion;
}

/**
 * Design 5.1 invariant 3: every logged transaction is whole — contiguous
 * positions from a `begin` to a `commit` that chains to the previous
 * transaction — and the sequence covers the committed watermarks with no gap.
 */
function expectContiguous(log: LogTransaction[], committed: string[]) {
  expect(log.map(({watermark}) => watermark)).toEqual(committed);
  for (const tx of log) {
    expect(tx.positions).toEqual(tx.positions.map((_, i) => i));
    expect(tx.tags.at(0)).toBe('begin');
    expect(tx.tags.at(-1)).toBe('commit');
    expect(tx.tags.slice(1, -1).includes('begin')).toBe(false);
    expect(tx.tags.slice(1, -1).includes('commit')).toBe(false);
    // Only the commit row carries a precommit, and it marks the transaction
    // boundary the reader validates a catchup range against.
    expect(tx.precommits.slice(0, -1).every(p => p === null)).toBe(true);
    expect(tx.precommits.at(-1)).toBe(tx.watermark);
  }
}

const files: DbFile[] = [];

afterEach(() => {
  let file: DbFile | undefined;
  while ((file = files.pop())) {
    deleteChangeLogDB(file.path);
    file.delete();
  }
});

function newReplica(): DbFile {
  const file = new DbFile('change-log-crash-recovery');
  files.push(file);
  const replica = file.connect(lc);
  replica.pragma('journal_mode = wal');
  initReplicationState(replica, ['zero_data'], REPLICA_VERSION);
  initDB(
    replica,
    `
    CREATE TABLE issues(
      id INTEGER PRIMARY KEY,
      text TEXT,
      _0_version TEXT
    );
    `,
  );
  replica.close();
  return file;
}

/** What the crash left behind, before and after recovery. */
type Outcome = {
  /** The log head and replica state as the recovering process found them. */
  readonly afterCrash: {logHead: string; stateVersion: string};
  readonly reconciled: ReconcileResult;
  readonly recoveredHead: string;
  readonly resumedHead: string;
};

function runScenario({before, changes, after, crashPoint}: Scenario): Outcome {
  const file = newReplica();
  const committed = [REPLICA_VERSION];
  const phantomExpected =
    crashPoint === 'state-update' || crashPoint === 'replica-commit';
  let next = 0;

  // The process that crashes.
  const crashed = startSession(file);
  try {
    expect(crashed.reconciled).toEqual({
      action: 'reseeded',
      head: REPLICA_VERSION,
      reason: 'created',
    });
    for (let i = 0; i < before; i++) {
      const wm = watermark(next++);
      transaction(crashed, wm, changes);
      committed.push(wm);
    }
    expect(crashed.failures).toEqual([]);

    const crashWatermark = watermark(next++);
    crashed.replicaRunner.crashOnStateUpdate = crashPoint === 'state-update';
    crashed.replicaRunner.crashOnCommit = crashPoint === 'replica-commit';
    crashed.changeLogRunner.crashOnCommit = crashPoint === 'log-commit';
    if (crashPoint === 'source-disconnect') {
      process(crashed, [
        'begin',
        issues.begin(),
        {commitWatermark: crashWatermark},
      ]);
      for (let i = 0; i < changes; i++) {
        process(crashed, ['data', issues.insert('issues', {id: i, text: 'x'})]);
      }
      crashed.processor.abort(lc);
    } else {
      transaction(crashed, crashWatermark, changes);
    }
    // An abort is expected, so it is not reported to the service; an injected
    // crash is.
    expect(crashed.failures).toHaveLength(
      crashPoint === 'source-disconnect' ? 0 : 1,
    );
  } finally {
    crashed.close();
  }

  // Invariant 1, observed before any repair: the log never trails the replica,
  // whatever the crash point.
  const afterCrash = (() => {
    using replica = new Database(lc, file.path, {readonly: true});
    using changeLog = openChangeLogDB(lc, file.path, {readonly: true});
    return {
      logHead: must(head(changeLog), 'the log is never empty'),
      stateVersion: stateVersion(replica),
    };
  })();
  expect(afterCrash.logHead >= afterCrash.stateVersion).toBe(true);
  expect(afterCrash.stateVersion).toBe(committed.at(-1));
  // A crash after the log's commit leaves the phantom behind.
  expect(afterCrash.logHead).toBe(
    phantomExpected ? watermark(next - 1) : afterCrash.stateVersion,
  );

  // The process that recovers.
  const recovered = startSession(file);
  try {
    expect(recovered.reconciled).toEqual(
      phantomExpected
        ? {
            action: 'truncated',
            head: committed.at(-1),
            rows: changes + 2, // begin + changes + commit
          }
        : {action: 'none', head: committed.at(-1)},
    );

    // Invariants 2 and 4.
    const recoveredHead = must(head(recovered.changeLog));
    expect(recoveredHead).toBe(stateVersion(recovered.replica));
    expect(
      recovered.changeLog
        .prepare(
          /*sql*/ `SELECT "replicaVersion" FROM "${CHANGE_LOG_META_TABLE}"`,
        )
        .get(),
    ).toEqual({replicaVersion: REPLICA_VERSION});
    expectContiguous(readLog(recovered.changeLog), committed);

    // Replication resumes from the recovered head.
    for (let i = 0; i < after; i++) {
      const wm = watermark(next++);
      transaction(recovered, wm, changes);
      committed.push(wm);
    }
    expect(recovered.failures).toEqual([]);
    const resumedHead = must(head(recovered.changeLog));
    expect(resumedHead).toBe(stateVersion(recovered.replica));
    expectContiguous(readLog(recovered.changeLog), committed);

    return {
      afterCrash,
      reconciled: recovered.reconciled,
      recoveredHead,
      resumedHead,
    };
  } finally {
    recovered.close();
  }
}

describe('replicator/change-log crash recovery', () => {
  test('a crash at any commit point leaves a phantom, never a hole', () => {
    fc.assert(
      fc.property(scenario, s => {
        runScenario(s);
      }),
      {numRuns: 40},
    );
  });

  test('a crash between the two commits leaves the log ahead by one', () => {
    const outcome = runScenario({
      before: 2,
      changes: 2,
      after: 2,
      crashPoint: 'state-update',
    });

    // '03' and '04' committed, the crash at '05'.
    expect(outcome.afterCrash).toEqual({logHead: '05', stateVersion: '04'});
    expect(outcome.reconciled).toEqual({
      action: 'truncated',
      head: '04',
      rows: 4,
    });
    expect(outcome.recoveredHead).toBe('04');
    expect(outcome.resumedHead).toBe('07');
  });

  test('an interrupted log commit leaves both databases at the same head', () => {
    const outcome = runScenario({
      before: 1,
      changes: 1,
      after: 1,
      crashPoint: 'log-commit',
    });

    expect(outcome.afterCrash).toEqual({logHead: '03', stateVersion: '03'});
    expect(outcome.reconciled).toEqual({action: 'none', head: '03'});
    expect(outcome.resumedHead).toBe('05');
  });

  test('a source disconnect mid-transaction leaves no trace', () => {
    const outcome = runScenario({
      before: 1,
      changes: 2,
      after: 1,
      crashPoint: 'source-disconnect',
    });

    expect(outcome.afterCrash).toEqual({logHead: '03', stateVersion: '03'});
    expect(outcome.reconciled).toEqual({action: 'none', head: '03'});
    expect(outcome.resumedHead).toBe('05');
  });

  test('a log left behind by a different replica is reseeded, never served', () => {
    const file = newReplica();
    const session = startSession(file);
    try {
      transaction(session, watermark(0), 2);
      expect(session.failures).toEqual([]);
    } finally {
      session.close();
    }

    // Simulate an auto-reset whose change-log deletion was missed: the replica
    // is re-synced at a new replicaVersion beside the old log.
    {
      using replica = new Database(lc, file.path);
      replica.exec(/*sql*/ `
        UPDATE "_zero.replicationConfig" SET "replicaVersion" = '04';
        UPDATE "_zero.replicationState" SET "stateVersion" = '04';
      `);
    }

    const reset = startSession(file);
    try {
      expect(reset.reconciled).toEqual({
        action: 'reseeded',
        head: '04',
        reason: 'replica-version-mismatch',
      });
      expectContiguous(readLog(reset.changeLog), ['04']);
    } finally {
      reset.close();
    }
  });
});
