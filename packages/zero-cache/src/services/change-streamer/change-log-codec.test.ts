import {describe, expect, test} from 'vitest';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  extractChangeSubstring,
  reconstructWatermarkedChange,
  serializeChangeStreamData,
  type ChangeLogEntry,
} from './change-log-codec.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer.ts';

type Fixture = {
  name: string;
  watermark: string;
  data: ChangeStreamData;
  json: string;
  change: string;
};

const fixtures: Fixture[] = [
  {
    name: 'begin',
    watermark: '01',
    data: [
      'begin',
      {tag: 'begin', json: 's', skipAck: true},
      {commitWatermark: '01'},
    ],
    json: '["begin",{"tag":"begin","json":"s","skipAck":true},{"commitWatermark":"01"}]',
    change: '{"tag":"begin","json":"s","skipAck":true}',
  },
  {
    name: 'data',
    watermark: '02',
    data: [
      'data',
      {
        tag: 'insert',
        relation: {
          schema: 'public',
          name: 'items',
          rowKey: {columns: ['id'], type: 'default'},
        },
        new: {
          id: 9007199254740993n,
          nested: {
            big: -9007199254740994n,
            values: [{}, []],
          },
          escapedNul: 'before\0after',
          quotes: '"quoted"',
          backslash: 'a\\b',
          unicode: '雪 ☃',
          emptyObject: {},
          emptyArray: [],
        },
      },
    ],
    json: '["data",{"tag":"insert","relation":{"schema":"public","name":"items","rowKey":{"columns":["id"],"type":"default"}},"new":{"id":9007199254740993,"nested":{"big":-9007199254740994,"values":[{},[]]},"escapedNul":"before\\u0000after","quotes":"\\"quoted\\"","backslash":"a\\\\b","unicode":"雪 ☃","emptyObject":{},"emptyArray":[]}}]',
    change:
      '{"tag":"insert","relation":{"schema":"public","name":"items","rowKey":{"columns":["id"],"type":"default"}},"new":{"id":9007199254740993,"nested":{"big":-9007199254740994,"values":[{},[]]},"escapedNul":"before\\u0000after","quotes":"\\"quoted\\"","backslash":"a\\\\b","unicode":"雪 ☃","emptyObject":{},"emptyArray":[]}}',
  },
  {
    name: 'schema',
    watermark: '03',
    data: [
      'data',
      {
        tag: 'rename-table',
        old: {schema: 'public', name: 'before'},
        new: {schema: 'archive', name: 'after'},
      },
    ],
    json: '["data",{"tag":"rename-table","old":{"schema":"public","name":"before"},"new":{"schema":"archive","name":"after"}}]',
    change:
      '{"tag":"rename-table","old":{"schema":"public","name":"before"},"new":{"schema":"archive","name":"after"}}',
  },
  {
    name: 'truncate',
    watermark: '04',
    data: [
      'data',
      {
        tag: 'truncate',
        relations: [
          {
            schema: 'public',
            name: 'items',
            rowKey: {columns: ['id'], type: 'default'},
          },
          {
            schema: 'public',
            name: 'events',
            rowKey: {columns: [], type: 'nothing'},
          },
        ],
      },
    ],
    json: '["data",{"tag":"truncate","relations":[{"schema":"public","name":"items","rowKey":{"columns":["id"],"type":"default"}},{"schema":"public","name":"events","rowKey":{"columns":[],"type":"nothing"}}]}]',
    change:
      '{"tag":"truncate","relations":[{"schema":"public","name":"items","rowKey":{"columns":["id"],"type":"default"}},{"schema":"public","name":"events","rowKey":{"columns":[],"type":"nothing"}}]}',
  },
  {
    name: 'backfill',
    watermark: '05',
    data: [
      'data',
      {
        tag: 'backfill',
        relation: {
          schema: 'public',
          name: 'items',
          rowKey: {columns: ['id'], type: 'default'},
        },
        columns: [],
        watermark: '04',
        rowValues: [
          [9007199254740995n, {nested: {big: 9007199254740997n}}, [], {}],
        ],
        status: {rows: 1, totalRows: 1, totalBytes: 0},
      },
    ],
    json: '["data",{"tag":"backfill","relation":{"schema":"public","name":"items","rowKey":{"columns":["id"],"type":"default"}},"columns":[],"watermark":"04","rowValues":[[9007199254740995,{"nested":{"big":9007199254740997}},[],{}]],"status":{"rows":1,"totalRows":1,"totalBytes":0}}]',
    change:
      '{"tag":"backfill","relation":{"schema":"public","name":"items","rowKey":{"columns":["id"],"type":"default"}},"columns":[],"watermark":"04","rowValues":[[9007199254740995,{"nested":{"big":9007199254740997}},[],{}]],"status":{"rows":1,"totalRows":1,"totalBytes":0}}',
  },
  {
    name: 'commit',
    watermark: '06',
    data: ['commit', {tag: 'commit'}, {watermark: '06'}],
    json: '["commit",{"tag":"commit"},{"watermark":"06"}]',
    change: '{"tag":"commit"}',
  },
  {
    name: 'rollback',
    watermark: '07',
    data: ['rollback', {tag: 'rollback'}],
    json: '["rollback",{"tag":"rollback"}]',
    change: '{"tag":"rollback"}',
  },
];

describe('change log codec', () => {
  test.each(fixtures)(
    '$name has an exact canonical representation',
    fixture => {
      const {watermark, data, json, change} = fixture;
      const tag = data[1].tag;
      const serialized = serializeChangeStreamData(data);

      expect(serialized).toBe(json);
      expect(extractChangeSubstring(serialized, tag)).toBe(change);

      const entry = {watermark, tag, change};
      expect(reconstructWatermarkedChange(entry)).toEqual([
        watermark,
        tag,
        json,
      ]);
    },
  );

  test.each(fixtures)(
    '$name preserves the pre-extraction PG representation',
    ({watermark, data}) => {
      const json = serializeChangeStreamData(data);
      const tag = data[1].tag;
      const change = extractChangeSubstring(json, tag);
      const entry = {watermark, tag, change};

      expect(change).toBe(legacyExtractChangeSubstring(json, tag));
      expect(reconstructWatermarkedChange(entry)).toEqual(
        legacyReconstructWatermarkedChange(entry),
      );
    },
  );
});

function legacyExtractChangeSubstring(
  streamMessageJSON: string,
  tag: ChangeTag,
): string {
  switch (tag) {
    case 'begin':
    case 'commit':
      return streamMessageJSON.substring(
        streamMessageJSON.indexOf(',') + 1,
        streamMessageJSON.lastIndexOf(','),
      );
    default:
      return streamMessageJSON.substring(
        streamMessageJSON.indexOf(',') + 1,
        streamMessageJSON.lastIndexOf(']'),
      );
  }
}

function legacyReconstructWatermarkedChange(
  entry: ChangeLogEntry,
): WatermarkedChange {
  const {watermark, change} = entry;
  const tag = entry.tag as ChangeTag;
  switch (tag) {
    case 'begin':
      return [
        watermark,
        tag,
        `["begin",${change},{"commitWatermark":"${watermark}"}]`,
      ];
    case 'commit':
      return [
        watermark,
        tag,
        `["commit",${change},{"watermark":"${watermark}"}]`,
      ];
    case 'rollback':
      return [watermark, tag, `["rollback",${change}]`];
    default:
      return [watermark, tag, `["data",${change}]`];
  }
}
