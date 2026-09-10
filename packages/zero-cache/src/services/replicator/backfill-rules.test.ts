// The subscriber half of resumable backfills: which `backfill` rows a replica
// is allowed to write, which `backfill-completed` it is allowed to honor, and
// how it records how far it has got with a run.
//
// The rules exist because a run can outlive the state it was started for. A
// subscriber that moves between replication-managers mid-run can be sent rows
// from an older snapshot than values it already holds, or a completion for
// rows it never received. Everything here is decided without ever comparing
// row keys: only Postgres orders keys, and a subscriber only ever compares a
// mark for equality.

import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {expectTables} from '../../test/lite.ts';
import type {
  BackfillCompleted,
  BackfillStarted,
  MessageBackfill,
  StreamedChange,
} from '../change-source/protocol/current/data.ts';
import {ChangeProcessor} from './change-processor.ts';
import {BACKFILLING_TABLE, readBackfillProgress} from './schema/backfilling.ts';
import {RESET_OP} from './schema/change-log.ts';
import {
  getSubscriptionState,
  initReplicationState,
} from './schema/replication-state.ts';
import {ReplicationMessages} from './test-utils.ts';

const RUN = 'run-1';
const OTHER_RUN = 'run-2';

/** The snapshot watermark of the runs below. */
const SNAPSHOT = '05';

const messages = new ReplicationMessages({issues: 'id'});

const relation: BackfillStarted['relation'] = {
  schema: 'public',
  name: 'issues',
  rowKey: {columns: ['id']},
};

describe('replicator/backfill rules', () => {
  let lc: LogContext;
  let replica: Database;
  let runner: StatementRunner;
  let processor: ChangeProcessor;
  let version = 2;

  /** Applies a transaction at the next watermark. */
  function tx(...changes: StreamedChange[]): void {
    txAt(lexi(++version), ...changes);
  }

  /** Applies a backfill transaction, whose version is replica-local. */
  function backfillTx(watermark: string, ...changes: StreamedChange[]): void {
    processor.processMessage(lc, [
      'begin',
      {tag: 'begin', skipAck: true, backfill: true},
      {commitWatermark: watermark},
    ]);
    for (const change of changes) {
      processor.processMessage(lc, ['data', change]);
    }
    processor.processMessage(lc, ['commit', {tag: 'commit'}, {watermark}]);
  }

  function txAt(watermark: string, ...changes: StreamedChange[]): void {
    processor.processMessage(lc, [
      'begin',
      {tag: 'begin'},
      {commitWatermark: watermark},
    ]);
    for (const change of changes) {
      processor.processMessage(lc, ['data', change]);
    }
    processor.processMessage(lc, ['commit', {tag: 'commit'}, {watermark}]);
  }

  function lexi(n: number): string {
    const base36 = n.toString(36);
    return `${(base36.length - 1).toString(36)}${base36}`;
  }

  function started(overrides: Partial<BackfillStarted> = {}): BackfillStarted {
    return {
      tag: 'backfill-started',
      relation,
      columns: ['description'],
      watermark: SNAPSHOT,
      runID: RUN,
      resumes: null,
      ...overrides,
    };
  }

  function backfill(overrides: Partial<MessageBackfill> = {}): MessageBackfill {
    return {
      tag: 'backfill',
      relation,
      columns: ['description'],
      watermark: SNAPSHOT,
      rowValues: [],
      runID: RUN,
      ...overrides,
    };
  }

  function completed(
    overrides: Partial<BackfillCompleted> = {},
  ): BackfillCompleted {
    return {
      tag: 'backfill-completed',
      relation,
      columns: ['description'],
      watermark: SNAPSHOT,
      runID: RUN,
      ...overrides,
    };
  }

  /** The replica's `_zero.backfilling` rows for `public.issues`. */
  function backfillingState() {
    return replica
      .prepare(
        `SELECT "column", "mark", "markWatermark", "runID", "runSeq",
                "minSnapshot"
           FROM "${BACKFILLING_TABLE}" ORDER BY "column"`,
      )
      .all();
  }

  function rows() {
    return replica
      .prepare(`SELECT id, description, note FROM issues ORDER BY id`)
      .all();
  }

  /** Whether the table's row versions were bumped (the IVM reset). */
  function resetCount() {
    const [{n}] = replica
      .prepare(
        `SELECT COUNT(*) AS n FROM "_zero.changeLog2"
           WHERE op = ? AND "table" = 'issues'`,
      )
      .all<{n: number}>(RESET_OP);
    return n;
  }

  beforeEach(() => {
    lc = createSilentLogContext();
    version = 2;
    replica = new Database(lc, ':memory:');
    initReplicationState(replica, ['zero_data'], '02');
    runner = new StatementRunner(replica);
    processor = new ChangeProcessor(runner, 'serving', (_, err) => {
      throw err;
    });

    // A synced table, and a column added to it whose backfill is in flight.
    tx(
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
    tx(messages.insert('issues', {id: 1, note: 'one'}));
    tx(messages.insert('issues', {id: 2, note: 'two'}));
    tx(
      messages.addColumn(
        'issues',
        'description',
        {dataType: 'text', pos: 2},
        {
          tableMetadata: {rowKey: {columns: ['id'], type: 'default'}},
          backfill: {issueID: 1},
        },
      ),
    );
  });

  describe('backfill-started decides whether the run is followed', () => {
    test('an announcement from the beginning covers everyone', () => {
      tx(started({resumes: null}));
      expect(backfillingState()).toEqual([
        {
          column: 'description',
          mark: null,
          markWatermark: null,
          runID: RUN,
          runSeq: 0,
          minSnapshot: null,
        },
      ]);
    });

    test('a run resuming the followed run from a batch applied is followed', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], seq: 1, lastKey: ['1']}));
      tx(backfill({rowValues: [[2, 'b']], seq: 2, lastKey: ['2']}));
      // A new run that picks up from where the manager's own replica got to,
      // which is at or below where this replica got to.
      tx(started({runID: OTHER_RUN, resumes: {runID: RUN, seq: 1}}));
      // The mark was this replica's in RUN, after batch 2. It says nothing
      // about OTHER_RUN's position 0, which starts from the manager's mark
      // after batch 1, so it goes with the run that produced it.
      expect(backfillingState()).toMatchObject([
        {mark: null, markWatermark: null, runID: OTHER_RUN, runSeq: 0},
      ]);
    });

    test('a run from the beginning replacing the followed run takes its mark with it', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], seq: 1, lastKey: ['1']}));
      tx(started({runID: OTHER_RUN, resumes: null}));
      expect(backfillingState()).toMatchObject([
        {mark: null, markWatermark: null, runID: OTHER_RUN, runSeq: 0},
      ]);
      // Resuming OTHER_RUN from its position 0 would skip rows at or below
      // RUN's mark, which a subscriber that followed only OTHER_RUN never
      // received.
      expect(readBackfillProgress(replica)).toMatchObject([
        {mark: null, runID: OTHER_RUN, runSeq: 0},
      ]);
    });

    test('a run resuming the followed run from a batch not applied is not followed', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], seq: 1, lastKey: ['1']}));
      // The manager's replica got further than this one before the resume.
      tx(started({runID: OTHER_RUN, resumes: {runID: RUN, seq: 2}}));
      expect(backfillingState()).toMatchObject([
        // A mark is only meaningful with the run and position that produced
        // it, so it goes with them.
        {mark: null, markWatermark: null, runID: null, runSeq: null},
      ]);
    });

    test('a run resuming a run that is not followed is not followed', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], seq: 1, lastKey: ['1']}));
      tx(started({runID: OTHER_RUN, resumes: {runID: 'elsewhere', seq: 0}}));
      expect(backfillingState()).toMatchObject([
        {mark: null, markWatermark: null, runID: null, runSeq: null},
      ]);
    });

    test('a re-announcement of the run being followed keeps following it', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], seq: 1, lastKey: ['1']}));
      // Re-delivered after a reconnect: the position is kept.
      tx(started({resumes: null}));
      expect(backfillingState()).toMatchObject([{runID: RUN, runSeq: 1}]);

      // And even an announcement resuming some other run, as long as it is
      // the same run: the replica is already following it.
      tx(started({resumes: {runID: 'elsewhere', seq: 9}}));
      expect(backfillingState()).toMatchObject([{runID: RUN, runSeq: 1}]);
    });

    test('a column that is not in flight here is untouched', () => {
      tx(started({columns: ['not-backfilling-here']}));
      expect(backfillingState()).toMatchObject([{runID: null}]);
    });
  });

  describe('the column guard', () => {
    test('rows are applied for in-flight columns', () => {
      tx(started());
      tx(
        backfill({
          rowValues: [
            [1, 'first'],
            [2, 'second'],
          ],
          lastKey: ['2'],
        }),
      );
      expect(rows()).toEqual([
        {id: 1, note: 'one', description: 'first'},
        {id: 2, note: 'two', description: 'second'},
      ]);
    });

    test('a message naming no in-flight column is skipped entirely', () => {
      tx(started());
      tx(completed()); // the backfill finishes

      // Scenario A: a run from an older snapshot, started by a
      // replication-manager this replica has since left, arrives with values
      // that are older than what the replica now holds.
      tx(messages.update('issues', {id: 1, note: 'one', description: 'new'}));
      const before = resetCount();

      tx(started({runID: OTHER_RUN}));
      tx(
        backfill({
          runID: OTHER_RUN,
          rowValues: [[1, 'stale']],
          lastKey: ['1'],
        }),
      );
      expect(rows()).toEqual([
        {id: 1, note: 'one', description: 'new'},
        {id: 2, note: 'two', description: null},
      ]);

      // ...and its completion neither completes anything nor resets the table
      // for IVM.
      tx(completed({runID: OTHER_RUN}));
      expect(resetCount()).toBe(before);
    });

    test('only in-flight columns are written', () => {
      tx(started());
      // A run carrying a column that this replica is not backfilling: `note`
      // is long since synced, and its value here is from an old snapshot.
      tx(
        backfill({
          columns: ['note', 'description'],
          rowValues: [[1, 'stale-note', 'first']],
          lastKey: ['1'],
        }),
      );
      expect(rows()).toEqual([
        {id: 1, note: 'one', description: 'first'},
        {id: 2, note: 'two', description: null},
      ]);
    });

    test('rows for a table this replica has not created yet are skipped', () => {
      // A run ahead of the replica. A replication-manager whose stream is
      // behind a subscriber that declared the table runs it at once, and a
      // replica behind that stream has not replicated the table's creation.
      const later: BackfillStarted['relation'] = {...relation, name: 'later'};
      tx(
        started({relation: later}),
        backfill({relation: later, rowValues: [[1, 'early']], lastKey: ['1']}),
      );
      expect(rows()).toEqual([
        {id: 1, note: 'one', description: null},
        {id: 2, note: 'two', description: null},
      ]);
    });
  });

  describe('marks advance only while following', () => {
    test('a followed run advances the mark', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], lastKey: ['1']}));
      expect(backfillingState()).toMatchObject([
        {mark: '["1"]', markWatermark: SNAPSHOT, runID: RUN},
      ]);
      tx(backfill({rowValues: [[2, 'b']], lastKey: ['2']}));
      expect(backfillingState()).toMatchObject([{mark: '["2"]'}]);
    });

    test('an unfollowed run does not', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], lastKey: ['1']}));
      tx(started({runID: OTHER_RUN, resumes: {runID: 'elsewhere', seq: 0}})); // not followed
      tx(backfill({runID: OTHER_RUN, rowValues: [[2, 'b']], lastKey: ['2']}));
      // The rows are still applied -- the column is in flight -- but no mark
      // is recorded for a run that is not followed.
      expect(rows()).toEqual([
        {id: 1, note: 'one', description: 'a'},
        {id: 2, note: 'two', description: 'b'},
      ]);
      expect(backfillingState()).toMatchObject([{mark: null, runID: null}]);
    });

    test('a run with no lastKey never advances the mark', () => {
      // A non-resumable or unordered run.
      tx(started());
      tx(backfill({rowValues: [[1, 'a']]}));
      expect(backfillingState()).toMatchObject([{mark: null}]);
    });

    test('the position advances with every batch, lastKey or not, and never back', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], seq: 1}));
      tx(backfill({rowValues: [[2, 'b']], seq: 2}));
      expect(backfillingState()).toMatchObject([{runSeq: 2, mark: null}]);
      // Re-delivered after a reconnect.
      tx(backfill({rowValues: [[1, 'a']], seq: 1}));
      expect(backfillingState()).toMatchObject([{runSeq: 2}]);
    });

    test('a batch that skips a position ends the following of the run', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], seq: 1, lastKey: ['1']}));
      // Batch 2 was sent while this replica was away, and came back on a
      // stream that starts after it.
      tx(backfill({rowValues: [[2, 'b']], seq: 3, lastKey: ['2']}));
      // The rows are still applied -- the column is in flight -- but the run
      // is no longer followed, and its mark goes with it.
      expect(rows()).toEqual([
        {id: 1, note: 'one', description: 'a'},
        {id: 2, note: 'two', description: 'b'},
      ]);
      expect(backfillingState()).toMatchObject([
        {mark: null, runID: null, runSeq: null},
      ]);
    });
  });

  describe('completion needs following', () => {
    test('a followed completion completes and resets the table', () => {
      const before = resetCount();
      tx(started());
      tx(completed());
      expect(backfillingState()).toEqual([]);
      expect(resetCount()).toBe(before + 1);
    });

    test('an unfollowed completion is ignored', () => {
      const before = resetCount();
      tx(started({runID: OTHER_RUN, resumes: {runID: 'elsewhere', seq: 0}}));
      // The announcement did not match, so nothing is following OTHER_RUN...
      tx(started()); // ...and this replica follows RUN instead.
      tx(completed({runID: OTHER_RUN}));
      expect(backfillingState()).toMatchObject([{runID: RUN}]);
      expect(resetCount()).toBe(before);
    });

    test('a completion at a position this replica has not reached is ignored', () => {
      const before = resetCount();
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], seq: 1}));
      // The run sent a second batch this replica never received.
      tx(completed({seq: 2}));
      expect(backfillingState()).toMatchObject([{runID: RUN, runSeq: 1}]);
      expect(resetCount()).toBe(before);
      // At the position of the run's last batch, it completes.
      tx(backfill({rowValues: [[2, 'b']], seq: 2}));
      tx(completed({seq: 2}));
      expect(backfillingState()).toEqual([]);
      expect(resetCount()).toBe(before + 1);
    });

    test('a completion from a change source that does not identify runs completes unconditionally', () => {
      const before = resetCount();
      tx(completed({runID: undefined}));
      expect(backfillingState()).toEqual([]);
      expect(resetCount()).toBe(before + 1);
    });

    test('a redundant completion is ignored even without a runID', () => {
      tx(completed({runID: undefined}));
      const before = resetCount();
      tx(completed({runID: undefined}));
      expect(resetCount()).toBe(before);
    });
  });

  describe('a key change voids marks, not runs', () => {
    test('the mark is cleared, minSnapshot is recorded and the run survives', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], lastKey: ['1']}));

      const keyChange = lexi(++version);
      txAt(
        keyChange,
        messages.update(
          'issues',
          {id: 3, note: 'one', description: 'a'},
          {id: 1},
        ),
      );

      expect(backfillingState()).toEqual([
        {
          column: 'description',
          mark: null,
          markWatermark: null,
          // The run is kept: a run whose rows were all sent before the change
          // has no row that moved, so its completion is still valid.
          runID: RUN,
          runSeq: 0,
          minSnapshot: keyChange,
        },
      ]);
    });

    test('an update to a FULL table moving no row key column leaves the mark alone', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], lastKey: ['1']}));
      // pgoutput names every column of a FULL table as a key column, and
      // sends the whole old row with every update.
      tx({
        tag: 'update',
        relation: {
          schema: 'public',
          name: 'issues',
          rowKey: {columns: ['id', 'note', 'description'], type: 'full'},
        },
        key: {id: 1, note: 'one', description: 'a'},
        new: {id: 1, note: 'changed', description: 'a'},
      });
      expect(backfillingState()).toMatchObject([
        {mark: '["1"]', minSnapshot: null},
      ]);
    });

    test('a redefined row key clears the mark and records minSnapshot', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], lastKey: ['1']}));
      const redefined = lexi(++version);
      txAt(redefined, {
        tag: 'update-table-metadata',
        table: {schema: 'public', name: 'issues'},
        old: {rowKey: {id: {attNum: 0}}},
        new: {rowKey: {note: {attNum: 1}}},
      });
      expect(backfillingState()).toMatchObject([
        {mark: null, markWatermark: null, runID: RUN, minSnapshot: redefined},
      ]);
    });

    test('new metadata with the same row key leaves the mark alone', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], lastKey: ['1']}));
      tx({
        tag: 'update-table-metadata',
        table: {schema: 'public', name: 'issues'},
        old: {rowKey: {id: {attNum: 0}}},
        new: {rowKey: {id: {attNum: 0}}, relationOID: 7},
      });
      expect(backfillingState()).toMatchObject([
        {mark: '["1"]', minSnapshot: null},
      ]);
    });

    test('an update that does not move the key leaves the mark alone', () => {
      tx(started());
      tx(backfill({rowValues: [[1, 'a']], lastKey: ['1']}));
      tx(
        messages.update(
          'issues',
          {id: 1, note: 'changed', description: 'a'},
          {id: 1},
        ),
      );
      expect(backfillingState()).toMatchObject([{mark: '["1"]'}]);
    });
  });

  describe('replica-local backfill versions', () => {
    function stateVersion() {
      return getSubscriptionState(runner).watermark;
    }

    test('an incoming version above the replica state is used as is', () => {
      const before = stateVersion();
      backfillTx(`${before}.01`, started());
      expect(stateVersion()).toBe(`${before}.01`);
    });

    test('versions are monotone when a second manager restarts its minors', () => {
      const major = stateVersion();
      // Manager A's transactions.
      backfillTx(`${major}.01`, started());
      backfillTx(`${major}.02`, backfill({rowValues: [[1, 'a']]}));
      backfillTx(`${major}.03`, backfill({rowValues: [[2, 'b']]}));
      expect(stateVersion()).toBe(`${major}.03`);

      // Manager B mints its own `M.1` for different rows. Applied at the
      // incoming version it would move the replica backwards, and handed back
      // to a change-streamer it would mean something else entirely.
      backfillTx(`${major}.01`, started({runID: OTHER_RUN}));
      expect(stateVersion()).toBe(`${major}.04`);
      backfillTx(`${major}.02`, backfill({runID: OTHER_RUN, rowValues: []}));
      expect(stateVersion()).toBe(`${major}.05`);
    });

    test('a legacy backfill transaction replayed below the replica state is local too', () => {
      const major = stateVersion();
      // Written to the change log by a replication-manager from before
      // `backfill` was set on the begin, whose only marker is `skipAck`, and
      // applied by that version at the incoming watermarks.
      const legacyTx = (watermark: string, change: StreamedChange) => {
        processor.processMessage(lc, [
          'begin',
          {tag: 'begin', skipAck: true},
          {commitWatermark: watermark},
        ]);
        processor.processMessage(lc, ['data', change]);
        processor.processMessage(lc, ['commit', {tag: 'commit'}, {watermark}]);
      };
      legacyTx(`${major}.01`, backfill({rowValues: [[1, 'a']]}));
      legacyTx(`${major}.02`, backfill({rowValues: [[2, 'b']]}));
      expect(stateVersion()).toBe(`${major}.02`);

      // Subscribing at the major replays both.
      legacyTx(`${major}.01`, backfill({rowValues: [[1, 'a']]}));
      expect(stateVersion()).toBe(`${major}.03`);
      legacyTx(`${major}.02`, backfill({rowValues: [[2, 'b']]}));
      expect(stateVersion()).toBe(`${major}.04`);
    });

    test('an ordinary transaction keeps the incoming version', () => {
      const major = stateVersion();
      backfillTx(`${major}.01`, started());
      const next = lexi(++version);
      txAt(next, messages.insert('issues', {id: 9, note: 'nine'}));
      expect(stateVersion()).toBe(next);
    });

    test('the backfilled row version is the snapshot watermark, not the local one', () => {
      const major = stateVersion();
      backfillTx(`${major}.01`, started());
      backfillTx(
        `${major}.02`,
        backfill({rowValues: [[3, 'c']], lastKey: ['3']}),
      );
      expectTables(replica, {
        issues: [
          {id: 1, note: 'one', description: null, ['_0_version']: '04'},
          {id: 2, note: 'two', description: null, ['_0_version']: '05'},
          {id: 3, note: null, description: 'c', ['_0_version']: SNAPSHOT},
        ],
      });
    });
  });

  test('re-delivering a run is idempotent', () => {
    const run = [
      started(),
      backfill({rowValues: [[1, 'a']], lastKey: ['1']}),
      backfill({rowValues: [[2, 'b']], lastKey: ['2']}),
    ];
    for (const change of run) {
      tx(change);
    }
    const afterFirst = {rows: rows(), state: backfillingState()};

    // A reconnect re-delivers everything the run sent.
    for (const change of run) {
      tx(change);
    }
    expect({rows: rows(), state: backfillingState()}).toEqual(afterFirst);

    tx(completed());
    expect(backfillingState()).toEqual([]);
  });
});
