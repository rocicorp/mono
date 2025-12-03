import {expect, test} from 'vitest';
import type {JSONValue} from '../../shared/src/json.ts';
import {must} from '../../shared/src/must.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {
  runBenchmarks,
  type PushGenerator,
} from '../../zql-integration-tests/src/helpers/runner.ts';
import type {PullRow} from '../../zql/src/query/query.ts';

const pgContent = await getChinook();

function defaultTrack(id: number): PullRow<'track', typeof schema> {
  return {
    id,
    name: `Track ${id}`,
    albumId: 248,
    mediaTypeId: 1,
    genreId: 1,
    composer: 'Composer',
    milliseconds: 1000,
    bytes: 1000,
    unitPrice: 1.99,
  };
}

let rawData: ReadonlyMap<string, readonly Row[]> = new Map();
await runBenchmarks(
  {
    suiteName: 'chinook_bench_push',
    type: 'push',
    pgContent,
    zqlSchema: schema,
    setRawData(raw) {
      rawData = raw;
    },
  },
  [
    {
      name: 'push into unlimited query',
      createQuery: q => q.track,
      generatePush: i => [
        [
          'track',
          {
            type: 'add',
            row: defaultTrack(i + 10_000),
          },
        ],
      ],
    },
    {
      name: 'push into limited query, outside the bound',
      createQuery: q => q.track.limit(100),
      generatePush: i => [
        [
          'track',
          {
            type: 'add',
            row: defaultTrack(i + 10_000),
          },
        ],
      ],
    },
    {
      name: 'push into limited query, inside the bound',
      createQuery: q => q.track.limit(100),
      generatePush: i => [
        [
          'track',
          {
            type: 'add',
            row: defaultTrack(-1 * i),
          },
        ],
      ],
    },
    {
      name: 'edit for limited query, outside the bound',
      createQuery: q => q.track.limit(100),
      generatePush: (() => {
        const edit = makeEdit();
        return (i: number) => {
          const change = edit(
            'track',
            // prettier does not respect order of operations below and formats
            // to the wrong equation.
            // prettier-ignore
            (i % ((must(rawData.get('track')).length - 100))) + 100,
            'name',
            Math.floor(Math.random() * 10_000) + '',
          );
          return [['track', change]];
        };
      })() satisfies PushGenerator,
    },
    {
      name: 'edit for limited query, inside the bound',
      createQuery: q => q.track.limit(100),
      generatePush: (() => {
        const edit = makeEdit();
        return (i: number) => [
          [
            'track',
            edit(
              'track',
              i % 100,
              'name',
              Math.floor(Math.random() * 10_000) + '',
            ),
          ],
        ];
      })(),
    },
  ],
);

function makeEdit() {
  const currentValues = new Map<string, Row>();
  return (table: string, index: number, column: string, value: JSONValue) => {
    const key = `${table}-${index}`;
    const dataset = rawData.get(table);
    if (!dataset) {
      throw new Error(`No dataset for table ${table}`);
    }
    const row = currentValues.get(key) ?? dataset[index];
    if (!row) {
      throw new Error(
        `No row for table ${table} at index ${index} ds length ${dataset.length}`,
      );
    }
    const newRow = {
      ...row,
      [column]: value,
    };
    currentValues.set(key, newRow);
    return {
      type: 'edit',
      oldRow: row,
      row: newRow,
    } as const;
  };
}

test('no-op', () => {
  expect(true).toBe(true);
});
