import {describe, expect, test} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import type {
  BackfillProgressMark,
  BackfillRequest,
  ChangeStreamData,
  MessageBackfill,
  BackfillCompleted,
} from '../change-source/protocol/current.ts';
import {BackfillState} from './backfill-state.ts';
import type {WatermarkedChange} from './change-streamer.ts';

function mark(progressMark: string, timeline = 't1'): BackfillProgressMark {
  return {progressMark, timeline};
}

const FOO = {schema: 'public', name: 'foo'};
const ROW_KEY = {columns: ['id'], type: 'default' as const};

function request(
  columns: Record<string, BackfillProgressMark | undefined>,
): BackfillRequest {
  return {
    table: {...FOO, metadata: null},
    columns: Object.fromEntries(
      Object.entries(columns).map(([col, progress]) => [
        col,
        progress ? {id: {col}, progress} : {id: {col}},
      ]),
    ),
  };
}

function backfill(
  columns: string[],
  progressMarks: MessageBackfill['progressMarks'],
): ChangeStreamData {
  return [
    'data',
    {
      tag: 'backfill',
      relation: {...FOO, rowKey: ROW_KEY},
      columns,
      watermark: '01',
      rowValues: [],
      progressMarks,
    },
  ];
}

function completed(
  columns: string[],
  progressMarks: BackfillCompleted['progressMarks'],
): ChangeStreamData {
  return [
    'data',
    {
      tag: 'backfill-completed',
      relation: {...FOO, rowKey: ROW_KEY},
      columns,
      watermark: '01',
      progressMarks,
    },
  ];
}

const BEGIN: ChangeStreamData = [
  'begin',
  {tag: 'begin'},
  {commitWatermark: '02'},
];
const COMMIT: ChangeStreamData = ['commit', {tag: 'commit'}, {watermark: '02'}];
const ROLLBACK: ChangeStreamData = ['rollback', {tag: 'rollback'}];

describe('change-streamer/backfill-state', () => {
  test('tracks contiguous progress and ignores gaps', () => {
    const state = new BackfillState([request({a: mark('05'), b: undefined})]);
    state.apply(BEGIN);
    // Contiguous for `a`, but `b` has no data yet (needs a run from scratch).
    expect(
      state.apply(
        backfill(['a', 'b'], {previous: mark('05'), current: mark('08')}),
      ),
    ).toEqual(['public.foo.b']);
    state.apply(COMMIT);
    expect(state.requests()).toEqual([request({a: mark('08'), b: undefined})]);

    // A run from scratch is accepted by both.
    state.apply(BEGIN);
    expect(state.apply(backfill(['a', 'b'], {current: mark('02')}))).toEqual(
      [],
    );
    state.apply(COMMIT);
    expect(state.requests()).toEqual([request({a: mark('02'), b: mark('02')})]);
  });

  test('completes only contiguous columns', () => {
    const state = new BackfillState([request({a: mark('05'), b: mark('02')})]);
    state.apply(BEGIN);
    expect(state.apply(completed(['a', 'b'], {previous: mark('04')}))).toEqual([
      'public.foo.b',
    ]);
    state.apply(COMMIT);
    expect(state.requests()).toEqual([request({b: mark('02')})]);

    state.apply(BEGIN);
    expect(state.apply(completed(['b'], {previous: mark('01')}))).toEqual([]);
    state.apply(COMMIT);
    expect(state.requests()).toEqual([]);
    expect(state.empty).toBe(true);
  });

  test('legacy messages are accepted', () => {
    const state = new BackfillState([request({a: mark('05')})]);
    state.apply(BEGIN);
    expect(state.apply(backfill(['a'], undefined))).toEqual([]);
    expect(state.apply(completed(['a'], undefined))).toEqual([]);
    state.apply(COMMIT);
    expect(state.requests()).toEqual([]);
  });

  test('schema changes', () => {
    const state = new BackfillState([request({a: mark('05'), b: mark('02')})]);
    state.apply(BEGIN);
    state.apply([
      'data',
      {
        tag: 'add-column',
        table: FOO,
        tableMetadata: {rowKey: {id: 1}},
        column: {name: 'c', spec: {pos: 3, dataType: 'text'}},
        backfill: {col: 'c'},
      },
    ]);
    state.apply([
      'data',
      {
        tag: 'update-column',
        table: FOO,
        old: {name: 'b', spec: {pos: 2, dataType: 'text'}},
        new: {name: 'bb', spec: {pos: 2, dataType: 'text'}},
      },
    ]);
    state.apply(['data', {tag: 'drop-column', table: FOO, column: 'a'}]);
    state.apply([
      'data',
      {tag: 'rename-table', old: FOO, new: {schema: 'public', name: 'bar'}},
    ]);
    state.apply([
      'data',
      {
        tag: 'create-table',
        spec: {schema: 'public', name: 'baz', columns: {}},
        metadata: {rowKey: {id: 2}},
        backfill: {x: {col: 'x'}},
      },
    ]);
    state.apply(COMMIT);

    expect(state.requests()).toEqual([
      {
        table: {schema: 'public', name: 'bar', metadata: {rowKey: {id: 1}}},
        columns: {
          bb: {id: {col: 'b'}, progress: mark('02')},
          c: {id: {col: 'c'}},
        },
      },
      {
        table: {schema: 'public', name: 'baz', metadata: {rowKey: {id: 2}}},
        columns: {x: {id: {col: 'x'}}},
      },
    ]);

    state.apply(BEGIN);
    state.apply([
      'data',
      {tag: 'drop-table', id: {schema: 'public', name: 'bar'}},
    ]);
    state.apply(COMMIT);
    expect(state.requests()).toEqual([
      {
        table: {schema: 'public', name: 'baz', metadata: {rowKey: {id: 2}}},
        columns: {x: {id: {col: 'x'}}},
      },
    ]);
  });

  test('rolled back changes are discarded', () => {
    const state = new BackfillState([request({a: mark('05')})]);
    state.apply(BEGIN);
    state.apply(backfill(['a'], {previous: mark('05'), current: mark('09')}));
    state.apply(['data', {tag: 'drop-table', id: FOO}]);
    // Uncommitted changes are not reflected.
    expect(state.requests()).toEqual([request({a: mark('05')})]);
    state.apply(ROLLBACK);
    expect(state.requests()).toEqual([request({a: mark('05')})]);

    // Subsequent transactions proceed from the pre-rollback state.
    state.apply(BEGIN);
    state.apply(backfill(['a'], {previous: mark('05'), current: mark('07')}));
    state.apply(COMMIT);
    expect(state.requests()).toEqual([request({a: mark('07')})]);
  });

  test('uncovered', () => {
    const session = new BackfillState([
      request({a: mark('03'), b: undefined, c: mark('05', 't2')}),
    ]);
    expect(
      new BackfillState([
        request({a: mark('04'), b: mark('01'), c: mark('06', 't2')}),
      ]).uncovered(session),
    ).toEqual([]);
    expect(
      new BackfillState([
        request({a: mark('02'), b: undefined, c: mark('06')}),
      ]).uncovered(session),
    ).toEqual(['public.foo.a', 'public.foo.c']);
    // A column that is not pending in the session is not covered.
    expect(
      new BackfillState([request({d: undefined})]).uncovered(session),
    ).toEqual(['public.foo.d']);
    // Nor is one with a different backfill ID.
    expect(
      new BackfillState([
        {
          table: {...FOO, metadata: null},
          columns: {b: {id: {other: true}}},
        },
      ]).uncovered(session),
    ).toEqual(['public.foo.b']);
  });

  test('applySerialized', () => {
    const state = new BackfillState([request({a: mark('05')})]);
    const serialize = (change: ChangeStreamData): WatermarkedChange => [
      '02',
      change[1].tag,
      BigIntJSON.stringify(change),
    ];
    state.applySerialized(serialize(BEGIN));
    expect(
      state.applySerialized(
        serialize(backfill(['a'], {previous: mark('06'), current: mark('09')})),
      ),
    ).toEqual(['public.foo.a']);
    state.applySerialized(
      serialize(backfill(['a'], {previous: mark('05'), current: mark('09')})),
    );
    // Irrelevant messages are not parsed.
    state.applySerialized(['02', 'insert', 'not json']);
    state.applySerialized(serialize(COMMIT));
    expect(state.requests()).toEqual([request({a: mark('09')})]);
  });

  test('withProgress', () => {
    const state = new BackfillState([
      request({a: mark('05'), b: mark('03'), c: undefined}),
    ]);
    // Uncommitted progress is not applied.
    state.apply(BEGIN);
    state.apply(backfill(['a'], {previous: mark('05'), current: mark('09')}));

    const reseeded: BackfillRequest[] = [
      {
        table: {...FOO, metadata: null},
        columns: {
          a: {id: {col: 'a'}}, // same backfill: progress carried over
          b: {id: {col: 'other'}}, // different backfill: from scratch
          c: {id: {col: 'c'}}, // no progress
          d: {id: {col: 'd'}}, // not tracked
        },
      },
      {
        table: {schema: 'public', name: 'bar', metadata: null},
        columns: {x: {id: {col: 'x'}}},
      },
    ];
    expect(state.withProgress(reseeded)).toEqual([
      {
        table: {...FOO, metadata: null},
        columns: {
          a: {id: {col: 'a'}, progress: mark('05')},
          b: {id: {col: 'other'}},
          c: {id: {col: 'c'}},
          d: {id: {col: 'd'}},
        },
      },
      {
        table: {schema: 'public', name: 'bar', metadata: null},
        columns: {x: {id: {col: 'x'}}},
      },
    ]);
  });
});
