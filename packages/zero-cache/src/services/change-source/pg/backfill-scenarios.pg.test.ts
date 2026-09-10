// End-to-end scenarios for resumable backfills: a real Postgres change source
// (manager, ordered COPY, run announcements) driving a real replica through
// `ChangeProcessor` (column guard, following rule, marks, local versions).
//
// Every case asserts the two things that matter regardless of what happened in
// between: the replica's rows end up equal to upstream's, and every column
// completes exactly once. A backfill that half-lands, or that completes twice
// and resets the table for IVM twice, fails both.

import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {BigIntJSON} from '../../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
import type {Database} from '../../../../../zqlite/src/db.ts';
import {
  getConnectionURI,
  type PgTest,
  test,
  testDBs,
} from '../../../test/db.ts';
import {DbFile} from '../../../test/lite.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {ChangeProcessor} from '../../replicator/change-processor.ts';
import {
  readBackfillDeclarations,
  readBackfillRequests,
  type BackfillDeclaration,
} from '../../replicator/schema/backfilling.ts';
import {createChangeProcessor} from '../../replicator/test-utils.ts';
import type {ChangeStream} from '../change-source.ts';
import type {
  BackfillRequest,
  ChangeStreamData,
  ChangeStreamMessage,
} from '../protocol/current.ts';
import {initializePostgresChangeSource} from './change-source.ts';

const APP_ID = 'bfr';
// Enough rows, and wide enough values, for the backfill to exceed
// COMMIT_THRESHOLD (below) several times over, so that the run spans more than
// one transaction. A run that commits only once has no interior point to be
// interrupted at, which is the whole subject of the resume scenarios.
const ROWS = 2_000;
const PAYLOAD_CHARS = 200;

/**
 * The manager's production threshold is 8MB, which would mean moving tens of
 * megabytes per case to get a second transaction out of it. Lowered here so
 * that the shape being tested -- an interrupted run -- costs a few hundred
 * kilobytes instead.
 */
const COMMIT_THRESHOLD = 64 * 1024;

describe('change-source/pg/backfill-scenarios', () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let upstreamURI: string;

  beforeEach<PgTest>(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('backfill_resume_e2e_upstream');
    upstreamURI = getConnectionURI(upstream);

    await upstream.unsafe(/*sql*/ `
      CREATE TABLE issues(
        id INT8 NOT NULL PRIMARY KEY,
        title TEXT NOT NULL,
        priority TEXT
      );
      INSERT INTO issues(id, title, priority)
        SELECT i, 'issue ' || i, repeat('p' || (i % 3), ${PAYLOAD_CHARS})
          FROM generate_series(1, ${ROWS}) i;
      -- priority starts outside the publication, so its values are never
      -- replicated. Adding it later is what needs a backfill.
      CREATE PUBLICATION zero_data FOR TABLE issues(id, title);
      ANALYZE issues;
    `);

    return async () => {
      await testDBs.drop(upstream);
    };
  }, 60_000);

  /**
   * One replication-manager session: a change source over the shared upstream,
   * and the replica it syncs. Sessions are started and stopped to simulate a
   * manager restart, and a replica can be handed from one to the next.
   */
  type Session = {
    stream: ChangeStream;
    downstream: Queue<ChangeStreamMessage | 'done'>;
    stop: () => Promise<void>;
  };

  const files: DbFile[] = [];

  function newReplica(name: string): DbFile {
    const file = new DbFile(`backfill_resume_e2e_${name}`);
    files.push(file);
    return file;
  }

  beforeEach(() => () => {
    files.splice(0).forEach(file => file.delete());
  });

  async function startSession(
    replicaFile: DbFile,
    watermark: string,
    backfillRequests: BackfillRequest[] = [],
  ): Promise<Session> {
    const {changeSource} = await initializePostgresChangeSource(
      lc,
      upstreamURI,
      {appID: APP_ID, publications: ['zero_data'], shardNum: 0},
      replicaFile.path,
      {tableCopyWorkers: 1},
      {test: 'backfill-resume'},
      0,
      {},
      {backupV5: true},
      null,
      undefined,
      {resume: true, commitThresholdBytes: COMMIT_THRESHOLD},
    );
    const stream = await changeSource.startStream(watermark, backfillRequests);
    const downstream = new Queue<ChangeStreamMessage | 'done'>();
    void (async () => {
      try {
        for await (const msg of stream.changes) {
          downstream.enqueue(msg);
        }
      } catch {
        // Cancelling the stream during teardown is expected.
      }
      downstream.enqueue('done');
    })();
    return {
      stream,
      downstream,
      stop: async () => {
        stream.changes.cancel();
        await changeSource.stop();
      },
    };
  }

  /**
   * Applies messages from `session` to `replicator` until `until` says to
   * stop. Returns what was applied.
   *
   * Throws rather than returning early if the stream goes quiet: a silent
   * timeout would surface later as a confusing diff against upstream rather
   * than as "the backfill never arrived".
   */
  async function apply(
    session: Session,
    replicator: ChangeProcessor,
    until: (msg: ChangeStreamMessage) => boolean,
  ): Promise<ChangeStreamMessage[]> {
    const applied: ChangeStreamMessage[] = [];
    for (;;) {
      const msg = await session.downstream.dequeue('done', 30_000);
      if (msg === 'done') {
        throw new Error(
          `the change stream went quiet after ` +
            `[${applied
              .map(m => (m[0] === 'data' ? m[1].tag : m[0]))
              .join(',')}]`,
        );
      }
      const [type] = msg;
      if (type !== 'control' && type !== 'status') {
        // Round-trip through JSON, as the wire does.
        replicator.processMessage(
          lc,
          BigIntJSON.parse(BigIntJSON.stringify(msg)) as ChangeStreamData,
        );
      }
      applied.push(msg);
      if (until(msg)) {
        return applied;
      }
    }
  }

  const isCompletion = (msg: ChangeStreamMessage) =>
    msg[0] === 'data' && msg[1].tag === 'backfill-completed';

  /** Every `backfill-completed` in a message sequence, by table and column. */
  function completions(messages: ChangeStreamMessage[]): string[] {
    return messages.filter(isCompletion).flatMap(msg => {
      const change = msg[1] as {
        relation: {schema: string; name: string};
        columns: string[];
      };
      return [...change.columns, 'title'].map(
        column => `${change.relation.name}.${column}`,
      );
    });
  }

  function rows(replica: Database) {
    return replica
      .prepare(`SELECT id, title, priority FROM issues ORDER BY id`)
      .all();
  }

  async function upstreamRows() {
    return (
      await upstream<{id: bigint; title: string; priority: string | null}[]>`
        SELECT id, title, priority FROM issues ORDER BY id`
    ).map(({id, title, priority}) => ({id, title, priority}));
  }

  /**
   * The replica's rows equal upstream's, and each column completed once. The
   * only two things every scenario has to end with.
   */
  async function expectSettled(
    replica: Database,
    applied: ChangeStreamMessage[],
  ) {
    const expected = await upstreamRows();
    expect(rows(replica)).toEqual(
      expected.map(({id, title, priority}) => ({
        id: Number(id),
        title,
        priority,
      })),
    );
    // Exactly one completion per column of the table.
    expect(completions(applied)).toEqual(['issues.priority', 'issues.title']);
    // ...and no backfill is left in flight.
    expect(readBackfillDeclarations(replica)).toEqual([]);
  }

  /**
   * Publishes a column that already has values in every row, which is what
   * makes a backfill necessary: replication will never deliver them.
   */
  const PUBLISH_COLUMN = /*sql*/ `
    ALTER PUBLICATION zero_data SET TABLE issues(id, title, priority);
  `;

  test('a backfill that runs to completion', async () => {
    const file = newReplica('simple');
    const replica = file.connect(lc);
    const session = await startSession(file, '00');
    const replicator = createChangeProcessor(replica);

    await upstream.unsafe(PUBLISH_COLUMN);
    const applied = await apply(session, replicator, isCompletion);

    await expectSettled(replica, applied);
    await session.stop();
    replica.close();
  }, 120_000);

  test('scenario E: a manager restart resumes from the replica mark', async () => {
    const file = newReplica('restart');
    let replica = file.connect(lc);
    let session = await startSession(file, '00');
    let replicator = createChangeProcessor(replica);

    await upstream.unsafe(PUBLISH_COLUMN);

    // Stop partway through the run: after the first batch of rows, but before
    // the completion.
    let sawRows = false;
    const first = await apply(session, replicator, msg => {
      if (msg[0] === 'data' && msg[1].tag === 'backfill') {
        sawRows = true;
      }
      // Stop at the commit that follows the first batch of rows.
      return sawRows && msg[0] === 'commit';
    });
    expect(first.some(isCompletion)).toBe(false);

    // The replica knows how far it got, and it is genuinely part-way: the run
    // was interrupted between transactions, not at its end.
    const declarations: BackfillDeclaration[] =
      readBackfillDeclarations(replica);
    expect(declarations).toHaveLength(1);
    const {mark, markWatermark} = must(declarations[0]);
    expect(mark).not.toBe(null);
    expect(Number(must(mark)[0])).toBeLessThan(ROWS);
    // Every row exists (it was replicated); only some have the backfilled
    // column so far.
    const backfilled = replica
      .prepare(`SELECT count(*) AS n FROM issues WHERE priority IS NOT NULL`)
      .get<{n: number}>().n;
    expect(backfilled).toBeGreaterThan(0);
    expect(backfilled).toBeLessThan(ROWS);

    const watermark = replica
      .prepare(`SELECT stateVersion FROM "_zero.replicationState"`)
      .get<{stateVersion: string}>().stateVersion;
    await session.stop();
    replica.close();

    // A new manager session, whose initial request carries the replica's mark
    // the way `withResumeMarks` supplies it.
    replica = file.connect(lc);
    replicator = createChangeProcessor(replica);
    // The initial requests a restarted manager gets, with the replica's mark
    // attached -- exactly what `withResumeMarks` builds in production.
    const requests = readBackfillRequests(replica).map(request => ({
      ...request,
      resumeFrom: mark,
      resumeFromWatermark: markWatermark,
    }));
    expect(requests).toHaveLength(1);
    session = await startSession(file, watermark.split('.')[0], requests);

    const resumed = await apply(session, replicator, isCompletion);
    // The resumed run announced itself from the mark rather than from zero.
    const announcement = resumed.find(
      msg => msg[0] === 'data' && msg[1].tag === 'backfill-started',
    );
    expect(
      announcement,
      `resumed messages: ${resumed
        .map(m => (m[0] === 'data' ? m[1].tag : m[0]))
        .join(',')}`,
    ).toBeDefined();
    expect(
      (announcement as ['data', {resumeFrom: string[] | null}])[1].resumeFrom,
    ).toEqual(mark);

    await expectSettled(replica, [...first, ...resumed]);
    await session.stop();
    replica.close();
  }, 120_000);

  test('scenario A: a stale run is ignored by a subscriber that finished', async () => {
    const file = newReplica('finished');
    const replica = file.connect(lc);
    const session = await startSession(file, '00');
    const replicator = createChangeProcessor(replica);

    await upstream.unsafe(PUBLISH_COLUMN);
    const applied = await apply(session, replicator, isCompletion);
    await expectSettled(replica, applied);

    // Upstream moves on: the column's values change after the backfill.
    await upstream.unsafe(/*sql*/ `UPDATE issues SET priority = 'new'`);
    // Checked only at transaction boundaries: stopping mid-transaction would
    // leave the processor inside one.
    await apply(
      session,
      replicator,
      msg =>
        msg[0] === 'commit' &&
        rows(replica).every(
          row => (row as {priority: string}).priority === 'new',
        ),
    );
    expect(
      rows(replica).every(
        row => (row as {priority: string}).priority === 'new',
      ),
    ).toBe(true);

    // A run from an older snapshot, of the kind another manager would send.
    // Its rows must not overwrite what replication has since delivered, and
    // its completion must not reset the table a second time.
    const stale = applied.filter(
      msg =>
        msg[0] === 'data' &&
        (msg[1].tag === 'backfill' ||
          msg[1].tag === 'backfill-started' ||
          msg[1].tag === 'backfill-completed'),
    );
    replicator.processMessage(lc, [
      'begin',
      {tag: 'begin', skipAck: true, backfill: true},
      {commitWatermark: '0z'},
    ]);
    for (const msg of stale) {
      replicator.processMessage(lc, msg as ChangeStreamData);
    }
    replicator.processMessage(lc, [
      'commit',
      {tag: 'commit'},
      {watermark: '0z'},
    ]);

    expect(rows(replica)).toEqual(
      (await upstreamRows()).map(({id, title, priority}) => ({
        id: Number(id),
        title,
        priority,
      })),
    );
    await session.stop();
    replica.close();
  }, 120_000);
});
