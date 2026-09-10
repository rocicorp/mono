import {describe, expect, expectTypeOf, test} from 'vitest';
import * as v from '../../../../../../shared/src/valita.ts';
import {
  isBackfillControl,
  isDataChange,
  isSchemaChange,
  type BackfillControl,
  type BackfillControlTag,
  type Change,
  type DataChange,
  type DataChangeTag,
  type SchemaChange,
  type SchemaChangeTag,
} from './data.ts';
import {changeStreamMessageSchema} from './downstream.ts';

test('schema and data change tags', () => {
  expectTypeOf<SchemaChangeTag>().toEqualTypeOf<SchemaChange['tag']>;
  expectTypeOf<DataChangeTag>().toEqualTypeOf<DataChange['tag']>;
  expectTypeOf<BackfillControlTag>().toEqualTypeOf<BackfillControl['tag']>;

  // Sanity check. The type check above should cover everything.
  for (const tag of [
    'create-table',
    'rename-table',
    'update-table-metadata',
    'add-column',
    'update-column',
    'drop-column',
    'drop-table',
    'create-index',
    'drop-index',
    'backfill-completed',
  ]) {
    expect(isSchemaChange({tag} as Change)).toBe(true);
    expect(isDataChange({tag} as Change)).toBe(false);
  }

  for (const tag of ['insert', 'update', 'backfill', 'truncate', 'delete']) {
    expect(isSchemaChange({tag} as Change)).toBe(false);
    expect(isDataChange({tag} as Change)).toBe(true);
  }

  // `backfill-started` is neither: it carries no rows and no DDL.
  expect(isSchemaChange({tag: 'backfill-started'} as Change)).toBe(false);
  expect(isDataChange({tag: 'backfill-started'} as Change)).toBe(false);
  expect(isBackfillControl({tag: 'backfill-started'} as Change)).toBe(true);

  for (const tag of ['begin', 'commit', 'status', 'rollback']) {
    expect(isSchemaChange({tag} as Change)).toBe(false);
    expect(isDataChange({tag} as Change)).toBe(false);
    expect(isBackfillControl({tag} as Change)).toBe(false);
  }
});

describe('protocol v7 messages', () => {
  const relation = {
    schema: 'public',
    name: 'issue',
    rowKey: {columns: ['id']},
  };

  test('backfill-started parses', () => {
    const msg = [
      'data',
      {
        tag: 'backfill-started',
        relation,
        columns: ['description'],
        watermark: '0a',
        runID: 'run-abc',
        resumeFrom: ['1234'],
      },
    ];
    expect(v.parse(msg, changeStreamMessageSchema, 'passthrough')).toEqual(msg);
  });

  test('backfill-started requires a runID and an explicit resumeFrom', () => {
    for (const change of [
      {tag: 'backfill-started', relation, columns: [], watermark: '0a'},
      {
        tag: 'backfill-started',
        relation,
        columns: [],
        watermark: '0a',
        resumeFrom: null,
      },
    ]) {
      expect(() =>
        v.parse(['data', change], changeStreamMessageSchema, 'passthrough'),
      ).toThrow();
    }
  });

  test('the new backfill fields are optional', () => {
    // A `backfill` replayed from a change log written before resumable
    // backfills has neither a runID nor a lastKey, and still parses.
    for (const change of [
      {
        tag: 'backfill',
        relation,
        columns: ['description'],
        watermark: '0a',
        rowValues: [['1', 'a']],
      },
      {
        tag: 'backfill',
        relation,
        columns: ['description'],
        watermark: '0a',
        rowValues: [['1', 'a']],
        runID: 'run-abc',
        lastKey: ['1'],
      },
      {
        tag: 'backfill-completed',
        relation,
        columns: ['description'],
        watermark: '0a',
      },
      {
        tag: 'backfill-completed',
        relation,
        columns: ['description'],
        watermark: '0a',
        runID: 'run-abc',
      },
    ]) {
      expect(
        v.parse(['data', change], changeStreamMessageSchema, 'passthrough'),
      ).toEqual(['data', change]);
    }
  });

  test('begin carries the backfill flag', () => {
    for (const begin of [
      {tag: 'begin'},
      {tag: 'begin', skipAck: true, backfill: true},
    ]) {
      const msg = ['begin', begin, {commitWatermark: '0a'}];
      expect(v.parse(msg, changeStreamMessageSchema, 'passthrough')).toEqual(
        msg,
      );
    }
  });

  test('an older peer passes the new fields through untouched', () => {
    // Passthrough parsing is what makes the additions safe in both
    // directions: a v6 peer neither drops nor rejects a v7 field.
    const msg = [
      'data',
      {
        tag: 'backfill',
        relation,
        columns: ['description'],
        watermark: '0a',
        rowValues: [['1', 'a']],
        runID: 'run-abc',
        lastKey: ['1'],
        someFutureField: 42,
      },
    ];
    expect(v.parse(msg, changeStreamMessageSchema, 'passthrough')).toEqual(msg);
  });
});
