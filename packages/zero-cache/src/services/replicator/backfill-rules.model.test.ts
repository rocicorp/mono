// An exhaustive-ish check of the subscriber half of resumable backfills,
// driven against the real `ChangeProcessor` and a real replica.
//
// `backfill-rules.test.ts` states the rules one at a time. This states the
// property those rules exist to produce, and lets a generator look for an
// interleaving that breaks it. The oracle is deliberately not a second
// implementation of the rules: it only knows what is true upstream, so a test
// failure means the replica disagrees with Postgres, not that it disagrees
// with a paraphrase.
//
// Two safety properties, from plans/resumable-backfills-plan.md section 4:
//
//   never stale  -- a backfilled column is either still empty or holds the
//                   current upstream value. A run whose snapshot predates a
//                   replicated write must never put the older value back
//                   (invariant 8, the column guard).
//   never half   -- when a completion is honored, every live row has its
//                   value. Honoring a completion for rows the replica never
//                   received publishes a column that is silently, permanently
//                   half empty (invariant 4).
//
// And one liveness property, which neither of those can see: a replica that
// stops following every run is never stale and never half.
//
//   completes    -- once the interleaving stops, and the subscriber stays with
//                   a manager that honors its backfill requests, the backfill
//                   completes.
//
// That puts the change-streamer's declaration tracker in the loop. It sees
// exactly what the replica sees, and its requests are the only way a replica
// that cannot follow a run gets one it can. The subscriber connects before the
// table exists and declares nothing, so the backfill it tracks is one that the
// stream itself started.
//
// Everything the generator emits is *manager-legal*: a run sends every row
// after its resume point, in key order, before it completes. What varies is
// which of those messages this subscriber sees -- which is what moving
// between replication-managers mid-run actually does to it.

import type {LogContext} from '@rocicorp/logger';
import fc from 'fast-check';
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {schemaVersionMigrationMap} from '../change-source/common/replica-schema.ts';
import type {
  BackfillCompleted,
  BackfillStarted,
  MessageBackfill,
  StreamedChange,
} from '../change-source/protocol/current/data.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {BackfillRequestMessage} from '../change-source/protocol/current/upstream.ts';
import {BackfillDeclarations} from '../change-streamer/backfill-declarations.ts';
import {ChangeProcessor} from './change-processor.ts';
import {
  BACKFILLING_TABLE,
  readBackfillDeclarations,
} from './schema/backfilling.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {ReplicationMessages} from './test-utils.ts';

const COLUMN = 'description';
const KEYS = [1, 2, 3] as const;
type Key = (typeof KEYS)[number];

const messages = new ReplicationMessages({issues: 'id'});

const relation: BackfillStarted['relation'] = {
  schema: 'public',
  name: 'issues',
  rowKey: {columns: ['id']},
};

/** What the generator may emit. Illegal steps are skipped, not forced. */
type Step =
  | {kind: 'startFromZero'}
  | {kind: 'startFromDeclaredMark'}
  | {kind: 'startFromKey'; key: Key}
  | {kind: 'sendBatch'; count: 1 | 2 | 3}
  | {kind: 'resendBatch'}
  | {kind: 'reannounce'}
  | {kind: 'complete'}
  // Any run this subscriber has seen re-delivers. `back: 0` is a reconnect to
  // the manager it is talking to now -- which, after the column has completed,
  // is the redundant backfill the column guard exists for. `back: 1` and above
  // are managers it has moved away from, which have not stopped.
  | {kind: 'staleResend'; back: 0 | 1 | 2}
  | {kind: 'staleComplete'; back: 0 | 1 | 2}
  | {kind: 'update'; id: Key}
  | {kind: 'delete'; id: Key}
  | {kind: 'keyChange'; id: Key; to: Key}
  // The same move, with the backfilled column left out of the replicated
  // update the way Postgres leaves out an unchanged TOASTed value. The moved
  // row arrives with no value for the column, so only a run that starts below
  // its new key will ever supply one.
  | {kind: 'keyChangeToasted'; id: Key; to: Key}
  // The subscriber's change stream reconnects, so the change-streamer's
  // tracker starts over from what the replica declares.
  | {kind: 'reconnect'}
  // A rollback to a v17 zero-cache, which replicates that TOASTed move but
  // maintains none of the resume columns, then the roll forward to v18.
  | {kind: 'v17KeyChangeToasted'; id: Key; to: Key};

const keyMove = fc
  .tuple(fc.constantFrom(...KEYS), fc.constantFrom(...KEYS))
  .filter(([id, to]) => id !== to);

const stepArb: fc.Arbitrary<Step> = fc.oneof(
  fc.constant<Step>({kind: 'startFromZero'}),
  fc.constant<Step>({kind: 'startFromDeclaredMark'}),
  fc.constantFrom(...KEYS).map<Step>(key => ({kind: 'startFromKey', key})),
  fc.constantFrom(1 as const, 2 as const, 3 as const).map<Step>(count => ({
    kind: 'sendBatch',
    count,
  })),
  fc.constant<Step>({kind: 'resendBatch'}),
  fc.constant<Step>({kind: 'reannounce'}),
  fc.constant<Step>({kind: 'complete'}),
  fc.constantFrom(0 as const, 1 as const, 2 as const).map<Step>(back => ({
    kind: 'staleResend',
    back,
  })),
  fc.constantFrom(0 as const, 1 as const, 2 as const).map<Step>(back => ({
    kind: 'staleComplete',
    back,
  })),
  fc.constantFrom(...KEYS).map<Step>(id => ({kind: 'update', id})),
  fc.constantFrom(...KEYS).map<Step>(id => ({kind: 'delete', id})),
  keyMove.map<Step>(([id, to]) => ({kind: 'keyChange', id, to})),
  keyMove.map<Step>(([id, to]) => ({kind: 'keyChangeToasted', id, to})),
  fc.constant<Step>({kind: 'reconnect'}),
  keyMove.map<Step>(([id, to]) => ({kind: 'v17KeyChangeToasted', id, to})),
);

/** A run in flight at some replication-manager. */
type Run = {
  runID: string;
  resumeFrom: Key | null;
  /** Keys the run still owes, in order. */
  pending: Key[];
  /** The values its COPY snapshot holds, taken when it started. */
  snapshot: Map<Key, string>;
  /** The watermark that snapshot was taken at. */
  watermark: string;
  /** The last batch it sent, for re-delivery after a reconnect. */
  lastBatch: {rowValues: unknown[][]; lastKey: string[]} | undefined;
  started: BackfillStarted;
  minor: number;
  /**
   * A row key change on the table cancels every run snapshotted before it
   * (`backfill-manager.ts:348`), so a canceled run sends no further data.
   * Its completion still stands: the check runs at the next data message, so
   * a run that had already sent everything completes normally, which is why
   * a key change voids marks but not runs.
   */
  canceled: boolean;
  /** Whether its completion has been sent. A manager re-announces no such run. */
  completed: boolean;
};

describe('replicator/backfill rules (model)', () => {
  let lc: LogContext;

  beforeEach(() => {
    lc = createSilentLogContext();
  });

  // The interleavings that motivated the rules, pinned so they are checked on
  // every run rather than only when the sweep below happens to generate them.
  // Each is manager-legal; what varies is what this subscriber sees.
  const SCENARIOS: Record<string, Step[]> = {
    // plan 7, Scenario A: the column completes, then the manager this
    // subscriber is talking to re-delivers the run after a reconnect. Without
    // the column guard the redundant rows overwrite the newer value, and
    // nothing records a per-column version once a backfill has completed.
    'a redundant backfill after completion': [
      {kind: 'startFromZero'},
      {kind: 'sendBatch', count: 3},
      {kind: 'complete'},
      {kind: 'update', id: 1},
      {kind: 'staleResend', back: 0},
    ],
    // A run resumed from another subscriber's mark completes rows this one
    // never received.
    'a completion for a run that was never followed': [
      {kind: 'startFromKey', key: 2},
      {kind: 'sendBatch', count: 3},
      {kind: 'complete'},
    ],
    // The subscriber moves to a manager whose run started elsewhere, and back.
    'a bounce between two managers mid-run': [
      {kind: 'startFromZero'},
      {kind: 'sendBatch', count: 1},
      {kind: 'startFromKey', key: 3},
      {kind: 'sendBatch', count: 3},
      {kind: 'complete'},
      {kind: 'startFromDeclaredMark'},
      {kind: 'sendBatch', count: 3},
      {kind: 'complete'},
    ],
    // A write that lands after the snapshot the run is reading from.
    'a replicated write racing the run that predates it': [
      {kind: 'startFromZero'},
      {kind: 'update', id: 1},
      {kind: 'sendBatch', count: 3},
      {kind: 'complete'},
    ],
    // A row whose key moves voids the mark but not the run.
    'a row key change under a run': [
      {kind: 'startFromZero'},
      {kind: 'sendBatch', count: 1},
      {kind: 'keyChange', id: 3, to: 1},
      {kind: 'startFromDeclaredMark'},
      {kind: 'sendBatch', count: 3},
      {kind: 'complete'},
    ],
    // The row moves down, past a cursor that has already gone by, and the
    // update that moved it carries no value for the column. Only voiding the
    // mark keeps a resumed run from starting above it and leaving it empty.
    'a row key change that omits an unchanged TOASTed value': [
      {kind: 'startFromZero'},
      {kind: 'sendBatch', count: 2},
      {kind: 'delete', id: 1},
      {kind: 'keyChangeToasted', id: 3, to: 1},
      {kind: 'startFromDeclaredMark'},
      {kind: 'sendBatch', count: 3},
      {kind: 'complete'},
    ],
    // A row deleted after the snapshot must not be resurrected.
    'a row deleted after the snapshot': [
      {kind: 'startFromZero'},
      {kind: 'delete', id: 2},
      {kind: 'sendBatch', count: 3},
      {kind: 'complete'},
    ],
    // Another subscriber's lower mark replaces the run, which this one, at
    // mark 2, cannot follow, and the replacement completes. The subscriber
    // declared nothing, so only the tracker's request for the backfill the
    // stream started gets it a run it can follow.
    'a replacement run it cannot follow, with nothing declared': [
      {kind: 'startFromZero'},
      {kind: 'sendBatch', count: 2},
      {kind: 'startFromKey', key: 1},
      {kind: 'sendBatch', count: 3},
      {kind: 'complete'},
    ],
    // The same move as above, replicated by a v17 zero-cache, which voids no
    // mark. Rolling forward has to forget the mark, or a run resumed from it
    // skips the moved row and leaves it empty.
    'a v17 rollback that replicates a row key change': [
      {kind: 'startFromZero'},
      {kind: 'sendBatch', count: 2},
      {kind: 'delete', id: 1},
      {kind: 'v17KeyChangeToasted', id: 3, to: 1},
      {kind: 'startFromDeclaredMark'},
      {kind: 'sendBatch', count: 3},
      {kind: 'complete'},
    ],
  };

  for (const [name, steps] of Object.entries(SCENARIOS)) {
    test(name, () => runScenario(lc, steps));
  }

  test('a backfilled column is never stale, never half populated, and completes', () => {
    fc.assert(
      fc.property(fc.array(stepArb, {minLength: 1, maxLength: 14}), steps => {
        runScenario(lc, steps);
      }),
      {numRuns: 5000, seed: 0x20260909},
    );
  });
});

function setUp(lc: LogContext) {
  const replica = new Database(lc, ':memory:');
  initReplicationState(replica, ['zero_data'], '02');
  const runner = new StatementRunner(replica);
  const processor = new ChangeProcessor(runner, 'serving', (_, err) => {
    throw err;
  });
  return {replica, processor};
}

function runScenario(lc: LogContext, steps: readonly Step[]) {
  const {replica, processor} = setUp(lc);
  let version = 2;
  const lexi = (n: number) => {
    const base36 = n.toString(36);
    return `${(base36.length - 1).toString(36)}${base36}`;
  };

  // The change-streamer's tracker for this subscriber, made the way
  // `Subscriber` makes it. The subscriber connects before the table exists,
  // so it declares nothing.
  let tracker = BackfillDeclarations.forSubscriber(true, []);
  const reconnect = () => {
    tracker = BackfillDeclarations.forSubscriber(
      true,
      readBackfillDeclarations(replica),
    );
  };

  function tx(
    watermark: string,
    backfill: boolean,
    ...changes: StreamedChange[]
  ) {
    const stream: ChangeStreamData[] = [
      [
        'begin',
        backfill
          ? {tag: 'begin', skipAck: true, backfill: true}
          : {tag: 'begin'},
        {commitWatermark: watermark},
      ],
      ...changes.map((change): ChangeStreamData => ['data', change]),
      ['commit', {tag: 'commit'}, {watermark}],
    ];
    for (const message of stream) {
      processor.processMessage(lc, message);
      tracker?.apply(message);
    }
  }

  // A run's snapshot is taken at an upstream commit watermark -- a major.
  // The minors below it are the replication-manager's own, and two managers
  // under the same major both mint `M.1, M.2, ...`.
  let lastMajor = lexi(version);
  const liveTx = (...changes: StreamedChange[]) => {
    lastMajor = lexi(++version);
    tx(lastMajor, false, ...changes);
  };

  // The bootstrap: a synced table, then a column whose backfill is in
  // flight. Only ADD COLUMN produces a backfill; rows inserted after CREATE
  // TABLE are in the WAL and replicate as ordinary inserts.
  liveTx(
    messages.createTable({
      schema: 'public',
      name: 'issues',
      primaryKey: ['id'],
      columns: {
        id: {dataType: 'int8', pos: 0, notNull: true},
        note: {dataType: 'text', pos: 1},
      },
    }),
    messages.createIndex({
      name: 'issues_pkey',
      schema: 'public',
      tableName: 'issues',
      unique: true,
      columns: {id: 'ASC'},
    }),
  );
  for (const id of KEYS) {
    liveTx(messages.insert('issues', {id, note: `note-${id}`}));
  }
  liveTx(
    messages.addColumn(
      'issues',
      COLUMN,
      {dataType: 'text', pos: 2},
      {
        tableMetadata: {rowKey: {columns: ['id'], type: 'default'}},
        backfill: {issueID: 1},
      },
    ),
  );

  // ---- the oracle: what is true upstream ----------------------------------
  /** The current value of the backfilled column, by live key. */
  const truth = new Map<Key, string>(KEYS.map(k => [k, `published-${k}`]));
  let writes = 0;

  // ---- the subscriber's view ----------------------------------------------
  // What a manager makes of this subscriber's declaration. A mark taken at a
  // snapshot older than the table's `minSnapshot` is dropped rather than
  // resumed from -- `change-log-initializer.ts:448` on the way in,
  // `backfill-manager.ts:552` at the manager.
  const declaredMark = (): Key | null => {
    const row = replica
      .prepare(
        `SELECT "mark", "markWatermark", "minSnapshot"
           FROM "${BACKFILLING_TABLE}" WHERE "column" = ?`,
      )
      .get<
        | {
            mark: string | null;
            markWatermark: string | null;
            minSnapshot: string | null;
          }
        | undefined
      >(COLUMN);
    if (!row?.mark) {
      return null;
    }
    if (row.minSnapshot && (row.markWatermark ?? '') < row.minSnapshot) {
      return null;
    }
    return Number(JSON.parse(row.mark)[0]) as Key;
  };
  const completed = () =>
    replica
      .prepare(`SELECT COUNT(*) AS n FROM "${BACKFILLING_TABLE}"`)
      .get<{n: number}>().n === 0;
  const followedRun = () =>
    replica
      .prepare(`SELECT "runID" FROM "${BACKFILLING_TABLE}" WHERE "column" = ?`)
      .get<{runID: string | null} | undefined>(COLUMN)?.runID ?? null;
  const rows = () =>
    new Map(
      replica
        .prepare(`SELECT id, ${COLUMN} AS v FROM issues`)
        .all<{id: number; v: string | null}>()
        .map(r => [r.id as Key, r.v]),
    );

  // Every run this subscriber has seen, oldest first. The last is the one
  // it is currently being sent; the others are runs at managers it has
  // moved away from, which have not stopped.
  const runs: Run[] = [];
  const current = () => runs.at(-1);
  const liveRun = () => {
    const run = current();
    return run && !run.canceled && !run.completed ? run : undefined;
  };
  let runCounter = 0;

  // What the manager that sent the last run knows: whether it still owes the
  // table a backfill, and the latest row key change on the table.
  let owed = true;
  let lastKeyChange = '';

  function check(after: string) {
    const actual = rows();
    for (const [id, value] of actual) {
      expect(
        truth.has(id),
        `${after}: row ${id} exists on the replica but not upstream`,
      ).toBe(true);
      if (value !== null) {
        // never stale
        expect(value, `${after}: row ${id} holds a stale value`).toBe(
          truth.get(id),
        );
      }
    }
    if (completed()) {
      for (const [id, value] of truth) {
        // never half
        expect(
          actual.get(id) ?? null,
          `${after}: run completed with row ${id} unpopulated`,
        ).toBe(value);
      }
    }
  }

  function startRun(resumeFrom: Key | null): Run {
    runCounter++;
    const watermark = lastMajor;
    const snapshot = new Map(truth);
    // KEYS is the key order Postgres would COPY in.
    const pending = KEYS.filter(
      k => truth.has(k) && (resumeFrom === null || k > resumeFrom),
    );
    const started: BackfillStarted = {
      tag: 'backfill-started',
      relation,
      columns: [COLUMN],
      watermark,
      runID: `run-${runCounter}`,
      resumeFrom: resumeFrom === null ? null : [String(resumeFrom)],
    };
    const run: Run = {
      runID: started.runID,
      resumeFrom,
      pending,
      snapshot,
      watermark,
      lastBatch: undefined,
      started,
      minor: 0,
      canceled: false,
      completed: false,
    };
    runs.push(run);
    owed = true;
    backfillTx(run, started);
    return run;
  }

  function backfillTx(r: Run, ...changes: StreamedChange[]) {
    r.minor++;
    tx(`${r.watermark}.${String(r.minor).padStart(2, '0')}`, true, ...changes);
  }

  function sendBatch(run: Run, count: number) {
    const batch = run.pending.splice(0, count);
    const rowValues = batch.map(k => [k, run.snapshot.get(k)!]);
    const lastKey = [String(batch.at(-1))];
    run.lastBatch = {rowValues, lastKey};
    backfillTx(run, backfillMsg(run, rowValues, lastKey));
  }

  function complete(run: Run) {
    run.completed = true;
    if (run === current()) {
      owed = false;
    }
    backfillTx(run, completedMsg(run));
  }

  function moveKey(id: Key, to: Key, toasted: boolean) {
    const value = truth.get(id)!;
    truth.delete(id);
    truth.set(to, value);
    liveTx(
      messages.update(
        'issues',
        toasted
          ? {id: to, note: `note-${id}`}
          : {id: to, note: `note-${id}`, [COLUMN]: value},
        {id},
      ),
    );
    lastKeyChange = lastMajor;
    for (const r of runs) {
      if (r.watermark < lastMajor) {
        r.canceled = true;
      }
    }
  }

  /**
   * The manager's side of a forwarded declaration, as
   * `BackfillManager.onBackfillRequest` decides it: nothing for a subscriber
   * already following the running run, a re-announcement from its mark when
   * the run has passed no rows it needs, a restart from its mark when it has,
   * and a run from the beginning for a table this session already finished.
   */
  function honor([, request]: BackfillRequestMessage) {
    let mark = request.mark === null ? null : (Number(request.mark[0]) as Key);
    if (mark !== null && (request.markWatermark ?? '') < lastKeyChange) {
      mark = null; // taken before a row key change on the table
    }
    if (!owed) {
      owed = true; // Scenario B: the next run starts from the beginning
      return;
    }
    const run = liveRun();
    if (run === undefined) {
      return; // nothing to compare against: the next run starts from zero
    }
    if (request.runID !== null && request.runID === run.runID) {
      return;
    }
    if (run.lastBatch === undefined) {
      startRun(null);
      return;
    }
    const lastMark = Number(run.lastBatch.lastKey[0]);
    const needed = [...truth.keys()].some(
      k => (mark === null || k > mark) && k <= lastMark,
    );
    if (needed) {
      startRun(mark);
    } else {
      backfillTx(run, {
        ...run.started,
        resumeFrom: mark === null ? null : [String(mark)],
      });
    }
  }

  for (const [i, step] of steps.entries()) {
    const at = `step ${i} (${step.kind})`;
    switch (step.kind) {
      case 'startFromZero':
        startRun(null);
        break;
      case 'startFromDeclaredMark':
        startRun(declaredMark());
        break;
      case 'startFromKey':
        startRun(step.key);
        break;
      case 'reannounce': {
        const run = liveRun();
        if (run) {
          backfillTx(run, run.started);
        }
        break;
      }
      case 'sendBatch': {
        const run = current();
        if (run && !run.canceled && run.pending.length > 0) {
          sendBatch(run, step.count);
        }
        break;
      }
      case 'resendBatch': {
        const run = current();
        if (run?.lastBatch && !run.canceled) {
          backfillTx(
            run,
            backfillMsg(run, run.lastBatch.rowValues, run.lastBatch.lastKey),
          );
        }
        break;
      }
      case 'complete': {
        // A manager only completes a run it has sent in full.
        const run = current();
        if (run && run.pending.length === 0) {
          complete(run);
        }
        break;
      }
      case 'staleResend': {
        const run = runs.at(-1 - step.back);
        if (run?.lastBatch && !run.canceled) {
          backfillTx(
            run,
            backfillMsg(run, run.lastBatch.rowValues, run.lastBatch.lastKey),
          );
        }
        break;
      }
      case 'staleComplete': {
        const run = runs.at(-1 - step.back);
        if (run && run.pending.length === 0) {
          complete(run);
        }
        break;
      }
      case 'update': {
        if (!truth.has(step.id)) {
          break;
        }
        const value = `written-${++writes}`;
        truth.set(step.id, value);
        liveTx(
          messages.update('issues', {
            id: step.id,
            note: `note-${step.id}`,
            [COLUMN]: value,
          }),
        );
        break;
      }
      case 'delete':
        if (!truth.has(step.id)) {
          break;
        }
        truth.delete(step.id);
        liveTx(messages.delete('issues', {id: step.id}));
        break;
      case 'keyChange':
      case 'keyChangeToasted':
        if (truth.has(step.id) && !truth.has(step.to)) {
          moveKey(step.id, step.to, step.kind === 'keyChangeToasted');
        }
        break;
      case 'reconnect':
        reconnect();
        break;
      case 'v17KeyChangeToasted': {
        if (!truth.has(step.id) || truth.has(step.to)) {
          break;
        }
        // Rolled back: the v17 zero-cache applies the move, and every resume
        // column the v18 rules would have changed keeps its old value.
        const kept = replica
          .prepare(
            `SELECT "column", "mark", "markWatermark", "runID", "minSnapshot"
               FROM "${BACKFILLING_TABLE}"`,
          )
          .all<{
            column: string;
            mark: string | null;
            markWatermark: string | null;
            runID: string | null;
            minSnapshot: string | null;
          }>();
        moveKey(step.id, step.to, true);
        const restore = replica.prepare(
          `UPDATE "${BACKFILLING_TABLE}"
             SET "mark" = ?, "markWatermark" = ?, "runID" = ?, "minSnapshot" = ?
             WHERE "column" = ?`,
        );
        for (const r of kept) {
          restore.run(
            r.mark,
            r.markWatermark,
            r.runID,
            r.minSnapshot,
            r.column,
          );
        }
        // Rolled forward: migration 18's `migrateData` runs again, and the
        // subscriber reconnects with what the replica then declares.
        void must(schemaVersionMigrationMap[18].migrateData)(lc, replica);
        reconnect();
        break;
      }
    }
    check(at);
  }

  // ---- completes ------------------------------------------------------------
  // The interleaving stops. The subscriber stays with the manager that sent
  // the last run, which honors its requests, and nothing changes upstream.
  const followed = followedRun();
  if (followed !== null && followed !== current()?.runID) {
    // It follows a run at a manager it has since left, so its connection to
    // this one is a new one, which starts from what the replica declares.
    reconnect();
  }
  for (let round = 0; round < 3 && !completed(); round++) {
    for (const request of tracker?.requests('subscriber') ?? []) {
      honor(request);
    }
    if (owed) {
      const run = liveRun() ?? startRun(null);
      if (run.pending.length > 0) {
        sendBatch(run, run.pending.length);
      }
      complete(run);
    }
    check(`after the interleaving, round ${round}`);
  }
  expect(
    completed(),
    'the backfill never completed after the interleaving stopped',
  ).toBe(true);
  replica.close();
}

function backfillMsg(
  run: Run,
  rowValues: unknown[][],
  lastKey: string[],
): MessageBackfill {
  return {
    tag: 'backfill',
    relation,
    columns: [COLUMN],
    watermark: run.watermark,
    rowValues: rowValues as MessageBackfill['rowValues'],
    runID: run.runID,
    lastKey,
  };
}

function completedMsg(run: Run): BackfillCompleted {
  return {
    tag: 'backfill-completed',
    relation,
    columns: [COLUMN],
    watermark: run.watermark,
    runID: run.runID,
  };
}
