/**
 * Property-based crash injection across the change log's commit ordering.
 *
 * The change-streamer commits the log before forwarding a transaction's
 * `commit` message, and everything that can advance the watermark the stream
 * would resume from — the storer's Postgres commit, `lastWatermark`, the
 * upstream ACK — runs strictly after that. So the log's head is always at or
 * above the resume point: a crash can leave a *phantom* (a transaction the
 * resumed stream will re-deliver) but never a *hole*, because a phantom is
 * detectable and truncatable while a hole would be served to a subscriber as a
 * silent gap.
 *
 * Each generated case drives a change stream through
 * {@link SQLiteChangeLogWriter}, kills the process at a chosen point relative to
 * the forward and the Postgres commit, then reopens the log exactly as the next
 * stream connection does and asserts the ordering invariant plus
 * transaction-level contiguity.
 *
 * The Postgres side is modeled rather than run: what the log is reconciled
 * against is a single watermark, and the only thing that matters about its
 * source is *when* it advances relative to the log's commit. The end-to-end
 * version of this, driven by a real storer failure, is in
 * `change-streamer/change-streamer-service.pg.test.ts`.
 */

import fc from 'fast-check';
import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../change-streamer/change-log-codec.ts';
import {SQLiteChangeLogWriter} from '../change-streamer/sqlite-change-log-writer.ts';
import {EMPTY_COOKIE_SET} from './change-log-cookies.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  deleteChangeLogDB,
  openChangeLogDB,
  readChangeLogHead,
  readChangeLogMeta,
  type ChangeLogIdentity,
} from './change-log-db.ts';
import {ReplicationMessages} from './test-utils.ts';

const lc = createSilentLogContext();
const issues = new ReplicationMessages({issues: 'id'});

const IDENTITY: ChangeLogIdentity = {
  epoch: null,
  generation: '02',
  replicaID: '1777575698286',
};

/** Where the process is killed, relative to the forward and the PG commit. */
type CrashPoint =
  // The log's COMMIT never runs: nothing for that transaction is durable.
  | 'before-log-commit'
  // The log is committed; the `commit` message is not yet forwarded.
  | 'after-log-commit'
  // The `commit` is forwarded; the resume watermark has not advanced.
  | 'after-forward'
  // Not a crash: the source drops mid-transaction and the writer rolls back.
  | 'source-disconnect'
  // Not a crash either: the whole transaction is logged and forwarded, and the
  // Postgres commit never lands. This is what a failed storer commit leaves,
  // and it is the in-process form of 'after-forward'.
  | 'no-pg-commit';

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
    'before-log-commit',
    'after-log-commit',
    'after-forward',
    'source-disconnect',
  ),
});

/** Lexicographic watermarks, in order, starting after the replica version. */
function watermark(i: number): string {
  return `0${'3456789abcdefghijk'[i]}`;
}

/**
 * One change-streamer process: the writer, the watermarks it has forwarded, and
 * the resume watermark that only a Postgres commit advances.
 */
class Session {
  readonly forwarded: string[] = [];
  readonly writer: SQLiteChangeLogWriter;
  #resumeWatermark: string;
  #dead = false;

  constructor(file: DbFile, resumeWatermark: string) {
    this.#resumeWatermark = resumeWatermark;
    this.writer = new SQLiteChangeLogWriter(lc, {
      replicaFile: file.path,
      identity: IDENTITY,
      now: () => 1_700_000_000_000,
    });
  }

  get resumeWatermark(): string {
    return this.#resumeWatermark;
  }

  reconcile() {
    // No backfill is in flight in any of these cases: what is under test is the
    // buffer's commit ordering, and the cookie set rides the same transaction.
    this.writer.reconcile({
      resumeWatermark: this.#resumeWatermark,
      cookies: EMPTY_COOKIE_SET,
    });
  }

  write(change: ChangeStreamData) {
    this.writer.write(change, serializeChangeStreamData(change));
  }

  forward(change: ChangeStreamData) {
    if (change[0] === 'commit') {
      this.forwarded.push(change[2].watermark);
    }
  }

  /** The storer's Postgres commit, i.e. the only thing that moves the anchor. */
  pgCommit(watermark: string) {
    this.#resumeWatermark = watermark;
  }

  /**
   * Kills the process. Closing the handle is what makes this a crash rather
   * than a shutdown: SQLite discards whatever the open transaction had not
   * committed, and nothing gets a chance to roll anything back.
   */
  kill() {
    if (!this.#dead) {
      this.#dead = true;
      this.writer.close();
    }
  }
}

class Crash extends Error {
  override readonly name = 'InjectedCrash';
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

/**
 * Every logged transaction is whole — contiguous positions from a `begin` to a
 * `commit` that chains to the previous transaction — and the sequence covers the
 * committed watermarks with no gap.
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

function newReplicaFile(): DbFile {
  const file = new DbFile('change-log-crash-recovery');
  files.push(file);
  return file;
}

/** Drives one whole transaction through the loop's order of operations. */
function transaction(
  session: Session,
  wm: string,
  changes: number,
  crashPoint?: CrashPoint,
): void {
  const messages: ChangeStreamData[] = [
    ['begin', issues.begin(), {commitWatermark: wm}],
    ...Array.from(
      {length: changes},
      (_, i) =>
        [
          'data',
          issues.insert('issues', {id: i, text: wm}),
        ] as ChangeStreamData,
    ),
  ];
  for (const message of messages) {
    session.write(message);
    session.forward(message);
  }

  if (crashPoint === 'source-disconnect') {
    session.writer.abort();
    return;
  }
  if (crashPoint === 'before-log-commit') {
    session.kill();
    throw new Crash('killed before the log commit');
  }

  const commit: ChangeStreamData = ['commit', issues.commit(), {watermark: wm}];
  session.write(commit);
  if (crashPoint === 'after-log-commit') {
    session.kill();
    throw new Crash('killed after the log commit, before the forward');
  }
  session.forward(commit);
  if (crashPoint === 'after-forward') {
    session.kill();
    throw new Crash('killed after the forward, before the PG commit');
  }
  if (crashPoint === 'no-pg-commit') {
    return;
  }
  session.pgCommit(wm);
}

/** What the crash left behind, before and after recovery. */
type Outcome = {
  readonly afterCrash: {logHead: string; resumeWatermark: string};
  readonly recoveredHead: string;
  readonly resumedHead: string;
};

function runScenario({before, changes, after, crashPoint}: Scenario): Outcome {
  const file = newReplicaFile();
  const REPLICA_VERSION = '02';
  const committed = [REPLICA_VERSION];
  const phantomExpected =
    crashPoint === 'after-log-commit' || crashPoint === 'after-forward';
  let next = 0;

  // The process that crashes.
  const crashed = new Session(file, REPLICA_VERSION);
  try {
    crashed.reconcile();
    expect(crashed.writer.enabled).toBe(true);
    for (let i = 0; i < before; i++) {
      const wm = watermark(next++);
      transaction(crashed, wm, changes);
      committed.push(wm);
    }

    const crashWatermark = watermark(next++);
    try {
      transaction(crashed, crashWatermark, changes, crashPoint);
      expect(crashPoint).toBe('source-disconnect');
    } catch (e) {
      expect(e).toBeInstanceOf(Crash);
    }
    // A write failure would have disabled the writer and deleted the file.
    // Nothing here is a write failure.
    expect(crashed.writer.state()?.invariantFailures ?? 0).toBe(0);
  } finally {
    crashed.kill();
  }

  const resumeWatermark = crashed.resumeWatermark;
  expect(resumeWatermark).toBe(committed.at(-1));

  // Invariant 1, observed before any repair: the log never trails the resume
  // watermark, whatever the crash point.
  const logHead = (() => {
    using changeLog = openChangeLogDB(lc, file.path, {readonly: true});
    return must(readChangeLogHead(changeLog), 'the log is never empty');
  })();
  expect(logHead >= resumeWatermark).toBe(true);
  expect(logHead).toBe(phantomExpected ? watermark(next - 1) : resumeWatermark);

  // The process that recovers. It reconciles against the watermark the stream
  // resumes from, which is where the crash left Postgres.
  const recovered = new Session(file, resumeWatermark);
  try {
    recovered.reconcile();
    expect(recovered.writer.enabled).toBe(true);

    using observer = openChangeLogDB(lc, file.path, {readonly: true});
    const recoveredHead = must(readChangeLogHead(observer));
    expect(recoveredHead).toBe(resumeWatermark);
    expect(readChangeLogMeta(observer)).toMatchObject({
      ...IDENTITY,
      // Never a reseed: the crash is always answered by truncate-above.
      seedWatermark: REPLICA_VERSION,
    });
    expectContiguous(readLog(observer), committed);

    // Replication resumes from the recovered head, re-delivering nothing and
    // colliding with nothing.
    for (let i = 0; i < after; i++) {
      const wm = watermark(next++);
      transaction(recovered, wm, changes);
      committed.push(wm);
    }
    expect(recovered.writer.state()?.invariantFailures ?? 0).toBe(0);
    expect(recovered.writer.enabled).toBe(true);
    const resumedHead = must(readChangeLogHead(observer));
    expect(resumedHead).toBe(recovered.resumeWatermark);
    expectContiguous(readLog(observer), committed);

    return {
      afterCrash: {logHead, resumeWatermark},
      recoveredHead,
      resumedHead,
    };
  } finally {
    recovered.kill();
  }
}

describe('replicator/change-log crash recovery', () => {
  test('a crash at any point leaves a phantom, never a hole', () => {
    fc.assert(
      fc.property(scenario, s => {
        runScenario(s);
      }),
      {numRuns: 40},
    );
  });

  test('a crash between the log commit and the forward leaves the log ahead by one', () => {
    const outcome = runScenario({
      before: 2,
      changes: 2,
      after: 2,
      crashPoint: 'after-log-commit',
    });

    // '03' and '04' committed, the crash at '05'.
    expect(outcome.afterCrash).toEqual({logHead: '05', resumeWatermark: '04'});
    expect(outcome.recoveredHead).toBe('04');
    expect(outcome.resumedHead).toBe('07');
  });

  test('a crash between the forward and the PG commit leaves the log ahead by one', () => {
    const outcome = runScenario({
      before: 1,
      changes: 1,
      after: 1,
      crashPoint: 'after-forward',
    });

    expect(outcome.afterCrash).toEqual({logHead: '04', resumeWatermark: '03'});
    expect(outcome.recoveredHead).toBe('03');
    expect(outcome.resumedHead).toBe('05');
  });

  test('an interrupted log commit leaves the log at the resume watermark', () => {
    const outcome = runScenario({
      before: 1,
      changes: 1,
      after: 1,
      crashPoint: 'before-log-commit',
    });

    expect(outcome.afterCrash).toEqual({logHead: '03', resumeWatermark: '03'});
    expect(outcome.resumedHead).toBe('05');
  });

  test('a source disconnect mid-transaction leaves no trace', () => {
    const outcome = runScenario({
      before: 1,
      changes: 2,
      after: 1,
      crashPoint: 'source-disconnect',
    });

    expect(outcome.afterCrash).toEqual({logHead: '03', resumeWatermark: '03'});
    expect(outcome.resumedHead).toBe('05');
  });

  // The in-process form of the same repair: no restart, just a stream
  // connection that resumes below the log's head. The writer inserts with a
  // plain INSERT, so an un-truncated overlap is a constraint violation on the
  // first re-delivered transaction.
  test('an in-process reconnect below the head truncates and re-accepts', () => {
    const file = newReplicaFile();
    const session = new Session(file, '02');
    try {
      session.reconcile();
      using observer = openChangeLogDB(lc, file.path, {readonly: true});

      // Two transactions reach the log; only the first reaches Postgres, which
      // is what a failed storer commit leaves behind.
      transaction(session, '03', 2);
      transaction(session, '05', 2, 'no-pg-commit');
      expect(readChangeLogHead(observer)).toBe('05');

      // The stream is re-established without a restart. Its resume watermark is
      // re-read, is behind the log's head, and reconciliation truncates '05'.
      session.writer.reconcile({
        resumeWatermark: '03',
        cookies: EMPTY_COOKIE_SET,
      });
      expect(readChangeLogHead(observer)).toBe('03');

      // The resumed stream re-delivers '05' at the same watermark.
      transaction(session, '05', 2);
      expect(session.writer.enabled).toBe(true);
      expect(session.writer.state()?.invariantFailures).toBe(0);
      expect(readChangeLogHead(observer)).toBe('05');
      expectContiguous(readLog(observer), ['02', '03', '05']);
    } finally {
      session.kill();
    }
  });
});
