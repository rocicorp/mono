import {expect, test} from 'vitest';
import {StatementRunner} from '../../db/statements.ts';
import {JSON_PARSED} from '../../types/lite.ts';
import {
  BackfillManager,
  type BackfillMessage,
} from '../change-source/common/backfill-manager.ts';
import {ChangeStreamMultiplexer} from '../change-source/common/change-stream-multiplexer.ts';
import {
  batch,
  lc,
  messages,
  relation,
  replica,
} from './backfill-repro-test-util.ts';
import {readBackfillRequests} from './schema/backfilling.ts';
import {getSubscriptionState} from './schema/replication-state.ts';

// These characterize current failures, rather than marking the tests as
// expected failures (which could hide an unrelated setup exception).
test.each(['backup', 'serving'] as const)(
  'repro: future backfill row wedges a key update (%s)',
  mode => {
    const r = replica(mode);
    try {
      // Snapshot 04 already contains the pending live update 1 -> 7.
      r.transaction('02.01', batch([7], '04'));
      expect(r.rows()).toEqual([
        {id: 1, label: 'row 1', body: null},
        {id: 7, label: null, body: 'body 7'},
      ]);
      expect(() =>
        r.transaction(
          '03',
          messages.update('items', {id: 7, label: 'row 1'}, {id: 1}),
        ),
      ).toThrow('UNIQUE constraint failed: items.id');
      // The failed transaction rolls back and the processor drops later input.
      r.transaction('05', messages.insert('items', {id: 9, label: 'later'}));
      expect(r.rows()).toHaveLength(2);
      expect(getSubscriptionState(new StatementRunner(r.db)).watermark).toBe(
        '02.01',
      );
    } finally {
      r.db.close();
    }
  },
);

test.each(['backup', 'serving'] as const)(
  'repro: future backfill conflicts with a secondary unique index (%s)',
  mode => {
    const r = replica(mode, [1, 2]);
    try {
      r.transaction(
        '03',
        batch([1], '03'),
        messages.createIndex({
          schema: 'public',
          name: 'unique_body',
          tableName: 'items',
          unique: true,
          columns: {body: 'ASC'},
        }),
      );
      // Upstream has cleared row 1's body and assigned it to row 2. Neither
      // update has reached the replica when the new snapshot's batch arrives.
      expect(() =>
        r.transaction('03.01', {
          ...batch([2], '05'),
          rowValues: [[2, 'body 1']],
        }),
      ).toThrow('UNIQUE constraint failed: items.body');
      expect(r.rows()).toEqual([
        {id: 1, label: 'row 1', body: 'body 1'},
        {id: 2, label: 'row 2', body: null},
      ]);
    } finally {
      r.db.close();
    }
  },
);

test.each(['backup', 'serving'] as const)(
  'repro: replicated insert silently replaces a backfilled row and loses its omitted body (%s)',
  mode => {
    const r = replica(mode, [2]);
    const body = 'unchanged toasted body '.repeat(1000);
    const rows = () =>
      r.db
        .prepare('SELECT id, label, handle, body FROM items ORDER BY id')
        .all();
    try {
      // body is already synced. Only handle will be backfilled in this repro.
      r.transaction(
        '03',
        {...batch([], '03'), rowValues: [[2, body]]},
        {
          tag: 'backfill-completed',
          relation,
          columns: ['body'],
          watermark: '03',
        },
      );
      r.transaction(
        '04',
        messages.addColumn(
          'items',
          'handle',
          {pos: 4, dataType: 'text'},
          {
            tableMetadata: {rowKey: {id: {attNum: 1}}},
            backfill: {attNum: 4},
          },
        ),
        messages.createIndex({
          schema: 'public',
          name: 'unique_handle',
          tableName: 'items',
          unique: true,
          columns: {handle: 'ASC'},
        }),
      );
      expect(rows()).toEqual([{id: 2, label: 'row 2', handle: null, body}]);

      // Snapshot 07 has seen INSERT 5, DELETE 5, UPDATE 2. Its batch arrives
      // before any of those live changes and gives row 2 the future handle.
      r.transaction('04.01', {
        tag: 'backfill',
        relation,
        columns: ['handle'],
        watermark: '07',
        rowValues: [[2, 'x']],
      });
      expect(rows()).toEqual([{id: 2, label: 'row 2', handle: 'x', body}]);

      r.transaction(
        '05',
        messages.insert('items', {
          id: 5,
          label: 'row 5',
          handle: 'x',
          body: 'body 5',
        }),
      );
      // REPLACE silently deletes row 2 through the secondary unique index.
      expect(rows()).toEqual([
        {id: 5, label: 'row 5', handle: 'x', body: 'body 5'},
      ]);
      expect(
        r.db
          .prepare(`
        SELECT rowKey, op FROM "_zero.changeLog2"
        WHERE "table" = 'items' AND stateVersion = '05' ORDER BY pos
      `)
          .all(),
      ).toEqual([{rowKey: '{"id":5}', op: 's'}]);

      r.transaction(
        '06',
        messages.delete('items', {id: 5}),
        // Simulate the unchanged-TOAST payload: body is absent, not null.
        messages.update('items', {id: 2, label: 'row 2', handle: 'x'}),
      );
      // UPDATE finds no row, so its fallback insert recreates it without body.
      expect(rows()).toEqual([
        {id: 2, label: 'row 2', handle: 'x', body: null},
      ]);
      r.transaction('07', {
        tag: 'backfill-completed',
        relation,
        columns: ['handle'],
        watermark: '07',
      });
      expect(readBackfillRequests(r.db)).toEqual([]);
      expect(rows()).toEqual([
        {id: 2, label: 'row 2', handle: 'x', body: null},
      ]);

      // This is silent loss, not a failed processor: subsequent input commits.
      r.transaction(
        '08',
        messages.insert('items', {
          id: 9,
          label: 'later',
          handle: 'y',
          body: 'body 9',
        }),
      );
      expect(rows()).toEqual([
        {id: 2, label: 'row 2', handle: 'x', body: null},
        {id: 9, label: 'later', handle: 'y', body: 'body 9'},
      ]);
      expect(getSubscriptionState(new StatementRunner(r.db)).watermark).toBe(
        '08',
      );
    } finally {
      r.db.close();
    }
  },
);

test.each(['backup', 'serving'] as const)(
  'repro: applying an old snapshot after a key change loses the body (%s)',
  mode => {
    const r = replica(mode, [5]);
    try {
      r.transaction(
        '04',
        messages.update('items', {id: 0, label: 'row 5'}, {id: 5}),
      );
      // Snapshot 03 contains only the old key. The processor correctly skips
      // its tombstone, but has no way to associate that body with the new key.
      // The manager guard tested below is necessary to prevent this sequence.
      r.transaction('04.01', batch([5], '03'), {
        tag: 'backfill-completed',
        relation,
        columns: ['body'],
        watermark: '03',
      });
      expect(r.rows()).toEqual([{id: 0, label: 'row 5', body: null}]);
      expect(readBackfillRequests(r.db)).toEqual([]);
    } finally {
      r.db.close();
    }
  },
);

test('in-progress key change: manager restarts the old snapshot and preserves the omitted body', async () => {
  const r = replica('backup', [5]);
  const mux = new ChangeStreamMultiplexer(lc, '02');
  let runs = 0;
  async function* snapshots(): AsyncGenerator<BackfillMessage> {
    runs++;
    const watermark = runs === 1 ? '03' : '05';
    yield {
      message: {
        ...batch([], watermark),
        rowValues: [[runs === 1 ? 5 : 0, 'body 5']],
      },
      byteSize: 1,
    };
    yield {
      message: {
        tag: 'backfill-completed',
        relation,
        columns: ['body'],
        watermark,
      },
      byteSize: 0,
    };
  }
  const manager = new BackfillManager(lc, mux, snapshots, JSON_PARSED);
  mux.addListeners(manager).addProducers(manager);
  const source = mux.asSource();
  try {
    // Hold the main reservation so the key change arrives after snapshot
    // creation but before its first batch. body is omitted, as with TOAST.
    await mux.reserve('main');
    manager.run('02', readBackfillRequests(r.db));
    void mux.push(['begin', {tag: 'begin'}, {commitWatermark: '04'}]);
    void mux.push([
      'data',
      messages.update('items', {id: 0, label: 'row 5'}, {id: 5}),
    ]);
    void mux.push(['commit', {tag: 'commit'}, {watermark: '04'}]);
    mux.release('04');
    mux.pushStatus(['status', {ack: false}, {watermark: '05'}]);
    for await (const msg of source) {
      if (msg[0] !== 'status' && msg[0] !== 'control') {
        r.processor.processMessage(lc, msg);
      }
      if (msg[0] === 'commit' && msg[2].watermark === '05') {
        break;
      }
    }
    expect(runs).toBe(2);
    expect(r.rows()).toEqual([{id: 0, label: 'row 5', body: 'body 5'}]);
    expect(readBackfillRequests(r.db)).toEqual([]);
  } finally {
    source.cancel();
    r.db.close();
  }
});
