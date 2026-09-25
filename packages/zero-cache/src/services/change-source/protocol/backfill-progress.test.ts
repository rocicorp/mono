import {describe, expect, test} from 'vitest';
import {
  acceptBackfill,
  covers,
  getSuperset,
  getTableSuperset,
  resumePoint,
  withResumePoint,
} from './backfill-progress.ts';
import type {BackfillProgressMark, BackfillRequest} from './current.ts';

function mark(progressMark: string, timeline = 't1'): BackfillProgressMark {
  return {progressMark, timeline};
}

function request(
  columns: Record<string, BackfillProgressMark | undefined>,
  name = 'foo',
): BackfillRequest {
  return {
    table: {schema: 'public', name, metadata: null},
    columns: Object.fromEntries(
      Object.entries(columns).map(([col, progress]) => [
        col,
        progress ? {id: {col}, progress} : {id: {col}},
      ]),
    ),
  };
}

describe('acceptBackfill', () => {
  test.each([
    [
      'legacy (no marks) is accepted without progress',
      mark('05'),
      undefined,
      {accept: true, progress: mark('05')},
    ],
    [
      'from scratch is always accepted',
      mark('05'),
      {current: mark('02')},
      {accept: true, progress: mark('02')},
    ],
    [
      'from scratch on a new timeline',
      mark('05'),
      {current: mark('02', 't2')},
      {accept: true, progress: mark('02', 't2')},
    ],
    [
      'from scratch without own progress',
      undefined,
      {current: mark('02')},
      {accept: true, progress: mark('02')},
    ],
    [
      'contiguous continuation',
      mark('05'),
      {previous: mark('05'), current: mark('08')},
      {accept: true, progress: mark('08')},
    ],
    [
      'overlapping continuation',
      mark('05'),
      {previous: mark('03'), current: mark('08')},
      {accept: true, progress: mark('08')},
    ],
    [
      'redundant continuation does not regress progress',
      mark('05'),
      {previous: mark('01'), current: mark('03')},
      {accept: true, progress: mark('05')},
    ],
    [
      'gap',
      mark('05'),
      {previous: mark('06'), current: mark('08')},
      {accept: false},
    ],
    [
      'continuation without own progress',
      undefined,
      {previous: mark('01'), current: mark('08')},
      {accept: false},
    ],
    [
      'continuation on another timeline',
      mark('05'),
      {previous: mark('01', 't2'), current: mark('08', 't2')},
      {accept: false},
    ],
    [
      'completion of a contiguous run',
      mark('05'),
      {previous: mark('05')},
      {accept: true, progress: mark('05')},
    ],
    [
      'completion of an empty run from scratch',
      undefined,
      {},
      {accept: true, progress: undefined},
    ],
    [
      'completion of a run with a gap',
      mark('05'),
      {previous: mark('09')},
      {accept: false},
    ],
  ] as const)('%s', (_, own, marks, expected) => {
    expect(acceptBackfill(own, marks)).toEqual(expected);
  });
});

describe('covers', () => {
  test.each([
    ['from scratch covers anything', undefined, mark('05'), true],
    ['from scratch covers from scratch', undefined, undefined, true],
    [
      'started stream does not cover from scratch',
      mark('01'),
      undefined,
      false,
    ],
    ['earlier stream covers', mark('03'), mark('05'), true],
    ['equal stream covers', mark('05'), mark('05'), true],
    ['later stream does not cover', mark('06'), mark('05'), false],
    ['other timeline does not cover', mark('01', 't2'), mark('05'), false],
  ] as const)('%s', (_, stream, subscriber, expected) => {
    expect(covers(stream, subscriber)).toBe(expected);
  });
});

describe('resumePoint', () => {
  test('earliest mark', () => {
    expect(resumePoint(request({a: mark('05'), b: mark('03')}))).toEqual(
      mark('03'),
    );
  });

  test('from scratch if any column has no progress', () => {
    expect(resumePoint(request({a: mark('05'), b: undefined}))).toBeUndefined();
  });

  test('from scratch if timelines differ', () => {
    expect(
      resumePoint(request({a: mark('05'), b: mark('03', 't2')})),
    ).toBeUndefined();
  });

  test('withResumePoint applies the resume point to all columns', () => {
    expect(withResumePoint(request({a: mark('05'), b: mark('03')}))).toEqual(
      request({a: mark('03'), b: mark('03')}),
    );
    expect(withResumePoint(request({a: mark('05'), b: undefined}))).toEqual(
      request({a: undefined, b: undefined}),
    );
  });
});

describe('getTableSuperset', () => {
  test('union of columns, earliest progress', () => {
    expect(
      getTableSuperset(
        request({a: mark('05'), b: mark('03', 'x')}),
        request({b: mark('02', 'x'), c: mark('09')}),
      ),
    ).toEqual(request({a: mark('05'), b: mark('02', 'x'), c: mark('09')}));
  });

  test('takes the whole (earlier) mark, including other fields', () => {
    const earlier = {progressMark: '01', timeline: 't1', extra: 'e1'};
    const later = {progressMark: '02', timeline: 't1', extra: 'e2'};
    expect(
      getTableSuperset(request({a: later}), request({a: earlier})),
    ).toEqual(request({a: earlier}));
  });

  test('from scratch for different timelines or missing progress', () => {
    expect(
      getTableSuperset(
        request({a: mark('05'), b: mark('03'), c: undefined}),
        request({a: mark('01', 't2'), b: undefined, c: mark('02')}),
      ),
    ).toEqual(request({a: undefined, b: undefined, c: undefined}));
  });

  test('from scratch for different backfill IDs', () => {
    const other: BackfillRequest = {
      table: {schema: 'public', name: 'foo', metadata: null},
      columns: {a: {id: {other: true}, progress: mark('01')}},
    };
    expect(getTableSuperset(request({a: mark('05')}), other)).toEqual(
      request({a: undefined}),
    );
  });

  test('metadata', () => {
    const withMetadata = (req: BackfillRequest, v: number) => ({
      ...req,
      table: {...req.table, metadata: {rowKey: {}, v}},
    });
    expect(
      getTableSuperset(
        request({a: undefined}),
        withMetadata(request({b: undefined}), 2),
      ).table.metadata,
    ).toEqual({rowKey: {}, v: 2});
    expect(
      getTableSuperset(
        withMetadata(request({a: undefined}), 1),
        withMetadata(request({b: undefined}), 2),
      ).table.metadata,
    ).toEqual({rowKey: {}, v: 1});
  });
});

test('getSuperset', () => {
  expect(
    getSuperset(
      [request({a: mark('05')}), request({x: mark('01')}, 'bar')],
      [],
      [request({a: mark('03'), b: undefined})],
    ),
  ).toEqual([
    request({a: mark('03'), b: undefined}),
    request({x: mark('01')}, 'bar'),
  ]);
});
