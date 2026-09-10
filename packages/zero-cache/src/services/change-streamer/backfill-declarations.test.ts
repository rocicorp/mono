import {describe, expect, test, vi} from 'vitest';
import type {Data} from '../change-source/protocol/current/downstream.ts';
import {BackfillDeclarations} from './backfill-declarations.ts';
import type {
  BackfillDeclaration,
  WatermarkedChange,
} from './change-streamer.ts';
import {createSubscriber} from './test-utils.ts';

const relation = {schema: 'public', name: 'issues', rowKey: {columns: ['id']}};
const metadata = {schemaOID: 1, relationOID: 2, rowKey: {id: {attNum: 1}}};
const declaration: BackfillDeclaration = {
  schema: 'public',
  table: 'issues',
  columns: ['body'],
  metadata,
  backfill: {body: {attNum: 2}},
  runID: 'old-run',
  runSeq: 3,
};
type Resumes = {runID: string; seq: number} | null;
const started = (runID = 'new-run', resumes: Resumes = null): Data => [
  'data',
  {
    tag: 'backfill-started',
    relation,
    columns: ['body'],
    watermark: '05',
    runID,
    resumes,
  },
];
const completed = (runID = 'new-run', seq?: number): Data => [
  'data',
  {
    tag: 'backfill-completed',
    relation,
    columns: ['body'],
    watermark: '05',
    runID,
    ...(seq === undefined ? {} : {seq}),
  },
];

function tx(tracker: BackfillDeclarations, ...changes: Data[]) {
  tracker.apply(['begin', {tag: 'begin'}, {commitWatermark: '06'}]);
  changes.forEach(change => tracker.apply(change));
  tracker.apply(['commit', {tag: 'commit'}, {watermark: '06'}]);
}

describe('backfill declaration recovery', () => {
  test('a completed manager is asked for the missing column with its original identities', () => {
    const tracker = new BackfillDeclarations([declaration]);
    tx(tracker, completed());
    expect(tracker.requests('sub')).toEqual([
      [
        'backfill-request',
        {
          table: {schema: 'public', name: 'issues', metadata},
          columns: {body: {attNum: 2}},
          runID: 'old-run',
          runSeq: 3,
          subscriberID: 'sub',
        },
      ],
    ]);
  });

  test.each<Resumes>([
    null,
    {runID: 'old-run', seq: 3},
    {runID: 'old-run', seq: 1},
  ])('a covering announcement %j suppresses the request', resumes => {
    const tracker = new BackfillDeclarations([declaration]);
    tx(tracker, started('new-run', resumes));
    expect(tracker.requests('sub')).toEqual([]);
    // The subscriber now follows the new run, from its start.
    expect(tracker.requests('sub', true)).toMatchObject([
      ['backfill-request', {runID: 'new-run', runSeq: 0}],
    ]);
    tx(tracker, completed());
    expect(tracker.requests('sub', true)).toEqual([]);
  });

  test.each<Resumes>([
    {runID: 'old-run', seq: 4},
    {runID: 'elsewhere', seq: 0},
  ])(
    'an announcement resuming %j does not cover, and takes the subscriber off its run',
    resumes => {
      const tracker = new BackfillDeclarations([declaration]);
      tx(tracker, started('new-run', resumes));
      expect(tracker.requests('sub')).toMatchObject([
        ['backfill-request', {runID: null, runSeq: null}],
      ]);
      tx(tracker, completed());
      expect(tracker.pending).toBe(true);
    },
  );

  test('an announcement for different columns cannot discharge the declaration', () => {
    const tracker = new BackfillDeclarations([declaration]);
    tx(tracker, [
      'data',
      {
        tag: 'backfill-started',
        relation,
        columns: ['title'],
        watermark: '05',
        runID: 'new-run',
        resumes: null,
      },
    ]);
    expect(tracker.requests('sub')).toHaveLength(1);
  });

  test('batches of the declared run cover it and advance its position', () => {
    const tracker = new BackfillDeclarations([declaration]);
    tx(tracker, batch('old-run', 4, ['7']));
    expect(tracker.requests('sub')).toEqual([]);
    expect(tracker.requests('sub', true)[0][1]).toMatchObject({
      runID: 'old-run',
      runSeq: 4,
    });
    // A run resuming it from that batch is followed.
    tx(tracker, started('new-run', {runID: 'old-run', seq: 4}));
    expect(tracker.requests('sub')).toEqual([]);
  });

  test('a batch that skips a position takes the subscriber off its run', () => {
    const tracker = new BackfillDeclarations([declaration]);
    tx(tracker, batch('old-run', 5, ['9']));
    expect(tracker.requests('sub')).toMatchObject([
      ['backfill-request', {runID: null, runSeq: null}],
    ]);
  });

  test('a completion at a position the subscriber has not reached does not discharge it', () => {
    const tracker = new BackfillDeclarations([declaration]);
    tx(tracker, completed('old-run', 4));
    expect(tracker.pending).toBe(true);
    expect(tracker.requests('sub')).toMatchObject([
      ['backfill-request', {runID: 'old-run', runSeq: 3}],
    ]);
    tx(tracker, completed('old-run', 3));
    expect(tracker.pending).toBe(false);
  });

  test('a rollback restores both the obligation and its identity', () => {
    const tracker = new BackfillDeclarations([declaration]);
    tracker.apply(['begin', {tag: 'begin'}, {commitWatermark: '06'}]);
    tracker.apply(started());
    tracker.apply(completed());
    tracker.apply(['rollback', {tag: 'rollback'}]);
    expect(tracker.requests('sub')[0][1]).toMatchObject({
      table: {name: 'issues', metadata},
      runID: 'old-run',
      runSeq: 3,
    });
  });

  test('table and column renames retain opaque metadata', () => {
    const tracker = new BackfillDeclarations([declaration]);
    tx(tracker, [
      'data',
      {
        tag: 'update-column',
        table: relation,
        old: {name: 'body', spec: {pos: 2, dataType: 'text'}},
        new: {name: 'content', spec: {pos: 2, dataType: 'text'}},
      },
    ]);
    tx(tracker, [
      'data',
      {
        tag: 'rename-table',
        old: relation,
        new: {schema: 'archive', name: 'issues'},
      },
    ]);
    expect(tracker.requests('sub')[0][1]).toMatchObject({
      table: {schema: 'archive', name: 'issues', metadata},
      columns: {content: {attNum: 2}},
    });
  });

  test('a dropped and recreated column is a new obligation', () => {
    const tracker = new BackfillDeclarations([
      {...declaration, columns: ['body', 'title']},
    ]);
    tx(
      tracker,
      ['data', {tag: 'drop-column', table: relation, column: 'body'}],
      [
        'data',
        {
          tag: 'add-column',
          table: relation,
          column: {name: 'body', spec: {pos: 4, dataType: 'text'}},
          backfill: {attNum: 4},
        },
      ],
    );
    expect(tracker.requests('sub')[0][1]).toMatchObject({
      columns: {body: {attNum: 4}},
      runID: null,
      runSeq: null,
    });
  });

  test('a table drop discharges the old declaration even if its name is reused', () => {
    const tracker = new BackfillDeclarations([declaration]);
    tx(
      tracker,
      ['data', {tag: 'drop-table', id: relation}],
      [
        'data',
        {
          tag: 'create-table',
          spec: {...relation, columns: {}},
          metadata: {...metadata, relationOID: 9},
          backfill: {body: {attNum: 2}},
        },
      ],
    );
    // The recreated table's backfill is the stream's own, and none of the old
    // declaration's identity carries over to it.
    expect(tracker.requests('sub')).toEqual([]);
    expect(tracker.requests('sub', true)).toEqual([
      [
        'backfill-request',
        {
          table: {
            schema: 'public',
            name: 'issues',
            metadata: {...metadata, relationOID: 9},
          },
          columns: {body: {attNum: 2}},
          runID: null,
          runSeq: null,
          subscriberID: 'sub',
        },
      ],
    ]);
  });

  test('row changes are not tracked: no mark is kept that a key change could void', () => {
    const tracker = new BackfillDeclarations([declaration]);
    expect(tracker.tracks('update')).toBe(false);
    expect(tracker.tracks('backfill')).toBe(true);
    expect(tracker.tracks('backfill-started')).toBe(true);
  });

  test('a backfill the stream starts is tracked from an empty declaration set', () => {
    const tracker = new BackfillDeclarations([]);
    expect(tracker.pending).toBe(false);

    tx(tracker, addColumn());
    expect(tracker.pending).toBe(true);
    // The source that sent the DDL holds the obligation, and the run it starts
    // announces itself, so there is nothing to request until then ...
    expect(tracker.requests('sub')).toEqual([]);
    // ... except from a new source session, as for any unfinished column.
    expect(tracker.requests('sub', true)).toEqual([
      [
        'backfill-request',
        {
          table: {schema: 'public', name: 'issues', metadata},
          columns: {body: {attNum: 2}},
          runID: null,
          runSeq: null,
          subscriberID: 'sub',
        },
      ],
    ]);

    tx(tracker, started('run-1', null));
    tx(tracker, batch('run-1', 1, ['2']));
    // Another manager's run replaces it, resuming a run this subscriber never
    // followed. This subscriber cannot follow the replacement, and neither
    // does its replica.
    tx(tracker, started('run-2', {runID: 'elsewhere', seq: 0}));
    expect(tracker.requests('sub')).toMatchObject([
      ['backfill-request', {runID: null, runSeq: null}],
    ]);
    tx(tracker, completed('run-2'));
    expect(tracker.requests('sub')).toMatchObject([
      ['backfill-request', {runID: null, runSeq: null}],
    ]);
  });

  test('a backfill started after the declared ones complete is tracked', () => {
    const tracker = new BackfillDeclarations([declaration]);
    tx(tracker, started('new-run', {runID: 'old-run', seq: 3}));
    tx(tracker, completed());
    expect(tracker.pending).toBe(false);

    tx(tracker, addColumn());
    tx(tracker, started('run-1', {runID: 'elsewhere', seq: 0}));
    expect(tracker.requests('sub')).toMatchObject([
      ['backfill-request', {columns: {body: {attNum: 2}}, runID: null}],
    ]);
  });
});

/** Adds the backfilled column, as the Postgres change source does. */
function addColumn(): Data {
  return [
    'data',
    {
      tag: 'add-column',
      table: relation,
      tableMetadata: metadata,
      column: {name: 'body', spec: {pos: 2, dataType: 'text'}},
      backfill: {attNum: 2},
    },
  ];
}

function batch(runID: string, seq: number, lastKey: string[]): Data {
  return [
    'data',
    {
      tag: 'backfill',
      relation,
      columns: ['body'],
      watermark: '05',
      runID,
      seq,
      lastKey,
      rowValues: [[lastKey[0], 'body']],
    },
  ];
}

describe('subscriber declaration handoff', () => {
  function setup() {
    const onBackfillRequests = vi.fn();
    const [subscriber, , downstream] = createSubscriber('03', false, {
      backfills: [declaration],
      onBackfillRequests,
    });
    const consuming = (async () => {
      for await (const _ of downstream) {
        /* consume */
      }
    })();
    return {
      subscriber,
      onBackfillRequests,
      close: async () => {
        subscriber.close();
        await consuming;
      },
    };
  }
  const streamTx = (
    watermark: string,
    ...data: Data[]
  ): WatermarkedChange[] => [
    [
      watermark,
      'begin',
      JSON.stringify(['begin', {tag: 'begin'}, {commitWatermark: watermark}]),
    ],
    ...data.map((d): WatermarkedChange => [
      watermark,
      d[1].tag,
      JSON.stringify(d),
    ]),
    [
      watermark,
      'commit',
      JSON.stringify(['commit', {tag: 'commit'}, {watermark}]),
    ],
  ];

  test('catchup and live backlog are folded before sending a request', async () => {
    const {subscriber, onBackfillRequests, close} = setup();
    for (const change of streamTx('05', completed())) {
      await subscriber.catchup(change);
    }
    for (const change of streamTx(
      '07',
      started('replacement', {runID: 'elsewhere', seq: 0}),
    )) {
      void subscriber.send(change);
    }
    await subscriber.setCaughtUp();
    expect(onBackfillRequests).toHaveBeenCalledTimes(1);
    expect(onBackfillRequests.mock.calls[0][0][0][1]).toMatchObject({
      runID: null,
      runSeq: null,
    });
    await close();
  });

  test('a covering live backlog suppresses requests and is re-declared on source restart', async () => {
    const {subscriber, onBackfillRequests, close} = setup();
    for (const change of streamTx('05', started())) {
      void subscriber.send(change);
    }
    await subscriber.setCaughtUp();
    expect(onBackfillRequests).not.toHaveBeenCalled();
    subscriber.requestBackfills(true);
    expect(onBackfillRequests).toHaveBeenCalledTimes(1);
    for (const change of streamTx('07', completed())) {
      await subscriber.send(change);
    }
    subscriber.requestBackfills(true);
    expect(onBackfillRequests).toHaveBeenCalledTimes(1);
    await close();
  });

  test('handoff waits for a partial live transaction to commit', async () => {
    const {subscriber, onBackfillRequests, close} = setup();
    const changes = streamTx('05', started());
    void subscriber.send(changes[0]);
    await subscriber.setCaughtUp();
    expect(onBackfillRequests).not.toHaveBeenCalled();
    await subscriber.send(changes[1]);
    await subscriber.send(changes[2]);
    expect(onBackfillRequests).not.toHaveBeenCalled();
    await close();
  });

  test('an unfollowed completion retries an identical outstanding request', async () => {
    const onBackfillRequests = vi.fn();
    const [subscriber, , downstream] = createSubscriber('03', false, {
      backfills: [declaration],
      onBackfillRequests,
    });
    const consuming = (async () => {
      for await (const _ of downstream) {
        /* consume */
      }
    })();
    await subscriber.setCaughtUp();
    for (const change of streamTx('05', completed())) {
      await subscriber.send(change);
    }
    expect(onBackfillRequests).toHaveBeenCalledTimes(2);
    expect(onBackfillRequests.mock.calls[0]).toEqual(
      onBackfillRequests.mock.calls[1],
    );
    subscriber.close();
    subscriber.requestBackfills(true);
    expect(onBackfillRequests).toHaveBeenCalledTimes(2);
    await consuming;
  });

  test.each([
    [7, 1],
    [6, 0],
  ])(
    'a v%s subscriber that declared nothing requests a run it cannot follow (%s request)',
    async (protocolVersion, requests) => {
      const onBackfillRequests = vi.fn();
      const [subscriber, , downstream] = createSubscriber(
        '03',
        true,
        {onBackfillRequests},
        'serving',
        protocolVersion,
      );
      const consuming = (async () => {
        for await (const _ of downstream) {
          /* consume */
        }
      })();
      await subscriber.setCaughtUp();
      for (const change of [
        ...streamTx('04', addColumn()),
        ...streamTx('05', started('run-1', null)),
        ...streamTx('05.01', batch('run-1', 1, ['2'])),
        ...streamTx('06', started('run-2', {runID: 'elsewhere', seq: 0})),
      ]) {
        await subscriber.send(change);
      }
      // A v6 subscriber never follows a run, so it has nothing to request.
      expect(onBackfillRequests).toHaveBeenCalledTimes(requests);
      if (requests) {
        expect(onBackfillRequests.mock.calls[0][0]).toMatchObject([
          ['backfill-request', {runID: null, runSeq: null}],
        ]);
      }
      subscriber.close();
      await consuming;
    },
  );
});
