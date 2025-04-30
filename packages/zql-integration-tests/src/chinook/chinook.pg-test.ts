/* eslint-disable @typescript-eslint/no-explicit-any */
import {test} from 'vitest';
import {createVitests} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import type {AnyQuery} from '../../../zql/src/query/test/util.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {SimpleOperator} from '../../../zero-protocol/src/ast.ts';
import type {PullRow} from '../../../zql/src/query/query.ts';

// Junction edges do not correctly handle limits in ZQL
const brokenRelationshipLimits = ['tracks'];

const pgContent = await getChinook();
const tables = Object.keys(schema.tables) as Array<keyof typeof schema.tables>;
let data: ReadonlyMap<string, readonly Row[]> | undefined;
type Schema = typeof schema;
type Rrc<T extends keyof Schema['tables']> = ReturnType<
  typeof randomRowAndColumn<T>
>;
const operators = ['=', '!=', '>', '>=', '<', '<='] as const;

test.each(
  await createVitests(
    {
      suiteName: 'compiler_chinook',
      pgContent,
      only: 'In operator',
      zqlSchema: schema,
      setRawData: r => {
        data = r;
      },
    },
    [
      {
        name: 'compare primary key',
        createQuery: q => q.track.where('id', '=', 2941),
      },
      {
        name: 'where equality',
        createQuery: q => q.album.where('title', 'Riot Act'),
        manualVerification: [
          {
            artistId: 118,
            id: 180,
            title: 'Riot Act',
          },
        ],
      },
      {
        name: '3 level related: customer -> supportRep -> reportsTo',
        createQuery: q =>
          q.invoice.related('customer', c =>
            c.related('supportRep', r => r.related('reportsTo')),
          ),
      },
      {
        name: 'Build a playlist',
        createQuery: q =>
          q.playlist.related('tracks', t =>
            t
              .related('mediaType')
              .related('genre')
              .related('album', a => a.related('artist')),
          ),
      },
      {
        name: '6 level related: Artist -> albums -> tracks -> invoiceLines -> invoices -> customer',
        createQuery: q =>
          q.artist.related('albums', a =>
            a.related('tracks', t =>
              t.related('invoiceLines', l =>
                l.related('invoice', i => i.related('customer')),
              ),
            ),
          ),
      },
      {
        name: 'Tracks that have been sold (exists testing)',
        createQuery: q => q.track.where(({exists}) => exists('invoiceLines')),
      },
      {
        name: 'Tracks that have not been sold (not exists testing)',
        createQuery: q =>
          q.track.where(({not, exists}) => not(exists('invoiceLines'))),
      },
      {
        name: 'Tracks sold or in a playlist (exists in or)',
        createQuery: q =>
          q.track.where(({or, exists}) =>
            or(exists('invoiceLines'), exists('playlists')),
          ),
      },
      {
        name: 'Tracks not sold and not in a playlist (not exists in and)',
        createQuery: q =>
          q.track.where(({and, not, exists}) =>
            and(not(exists('invoiceLines')), not(exists('playlists'))),
          ),
      },
      {
        name: 'Tracks sold and in a playlist (exists in and)',
        createQuery: q =>
          q.track.where(({and, exists}) =>
            and(exists('invoiceLines'), exists('playlists')),
          ),
      },
      {
        name: 'Prefix like',
        createQuery: q => q.album.where('title', 'LIKE', 'Riot%').limit(1),
        manualVerification: [
          {
            artistId: 118,
            id: 180,
            title: 'Riot Act',
          },
        ],
      },
      {
        name: 'Suffix like',
        createQuery: q => q.album.where('title', 'LIKE', '%Act').limit(1),
        manualVerification: [
          {
            artistId: 118,
            id: 180,
            title: 'Riot Act',
          },
        ],
      },
      {
        name: 'Contains like',
        createQuery: q => q.album.where('title', 'LIKE', '%Riot%').limit(1),
        manualVerification: [
          {
            artistId: 118,
            id: 180,
            title: 'Riot Act',
          },
        ],
      },
      {
        name: 'Not like',
        createQuery: q => q.album.where('title', 'NOT LIKE', '%Act').limit(1),
      },
      {
        name: 'In operator',
        createQuery: q =>
          q.album.where('title', 'IN', [
            'Riot Act',
            'For Those About To Rock We Salute You',
          ]),
        manualVerification: [
          {
            artistId: 1,
            id: 1,
            title: 'For Those About To Rock We Salute You',
          },
          {
            artistId: 118,
            id: 180,
            title: 'Riot Act',
          },
        ],
      },
      {
        name: 'Not in operator',
        createQuery: q =>
          q.album
            .where('title', 'NOT IN', [
              'Riot Act',
              'For Those About To Rock We Salute You',
            ])
            .limit(10),
      },
    ],
    // compare against primary key for each operator
    (() =>
      operators.map(
        op =>
          ({
            name: `track.where('id', '${op}', 2941)`,
            createQuery: q => q.track.where('id', op, 2941),
          }) as const,
      ))(),
    // same with limit 1
    (() =>
      operators.map(
        op =>
          ({
            name: `track.where('id', '${op}', 2941)`,
            createQuery: q => q.track.where('id', op, 2941).limit(1),
          }) as const,
      ))(),
    // SELECT * FROM <table> WHERE <column> = <value>
    (() =>
      tables.map(table => {
        let cached: Rrc<keyof Schema['tables']> | undefined;
        const rrc = () => cached ?? (cached = randomRowAndColumn(table));
        return {
          name: `${table}.where(someCol, 'someVal')`,
          createQuery: q =>
            (q[table] as AnyQuery).where(
              rrc().randomColumn,
              '=',
              rrc().randomRow[rrc().randomColumn] as any,
            ),
        } as const;
      }))(),
    // SELECT * FROM <table>
    (() =>
      tables.map(
        table =>
          ({
            name: `${table}`,
            createQuery: q => q[table],
          }) as const,
      ))(),
    // SELECT * FROM <table> LIMIT 100
    (() =>
      tables.map(
        table =>
          ({
            name: `${table}.limit(100)`,
            createQuery: q => q[table].limit(100),
          }) as const,
      ))(),
    // table.related('relationship')
    (() =>
      tables.flatMap(table =>
        getRelationships(table).map(
          relationship =>
            ({
              name: `${table}.related('${relationship}')`,
              createQuery: q => (q[table] as AnyQuery).related(relationship),
            }) as const,
        ),
      ))(),
    // table.related('relationship', q => q.limit(100))
    (() =>
      tables.flatMap(table =>
        getRelationships(table)
          .filter(r => !brokenRelationshipLimits.includes(r))
          .map(
            relationship =>
              ({
                name: `${table}.related('${relationship}', q => q.limit(100))`,
                createQuery: q =>
                  (q[table] as AnyQuery).related(relationship, q =>
                    q.limit(100),
                  ),
              }) as const,
          ),
      ))(),
    // OR tests
    [
      // unary or --
      // table.where(({or}) => or(cmp('col1', 'val1'))
      (() => {
        let cached: Rrc<'employee'> | undefined;
        const rrc = () => cached ?? (cached = randomRowAndColumn('employee'));
        return {
          name: 'unary or',
          createQuery: q => {
            const {randomRow, randomColumn} = rrc();
            return q.employee.where(({or, cmp}) =>
              or(cmp(randomColumn, '=', randomRow[randomColumn] as any)),
            );
          },
        };
      })(),
      // n-ary or
      (() => {
        const n = 5;
        let cached:
          | {rowsAndColumns: Array<Rrc<'artist'>>; operators: SimpleOperator[]}
          | undefined;
        const rrc = () =>
          cached ??
          (cached = {
            rowsAndColumns: Array.from({length: n}, () =>
              randomRowAndColumn('artist'),
            ),
            operators: Array.from({length: n}, () => randomOperator()),
          });
        return {
          name: 'n-branches',
          createQuery: q => {
            const {rowsAndColumns, operators} = rrc();
            return q.artist.where(({or, cmp}) =>
              or(
                ...rowsAndColumns.map(({randomRow, randomColumn}, i) =>
                  cmp(
                    randomColumn as any,
                    operators[i],
                    randomRow[randomColumn],
                  ),
                ),
              ),
            );
          },
        };
      })(),
      // contradictory branches.
      // table.where(({or}) => or(cmp('col1', '=', 'val1'), cmp('col1', '!=', 'val1')))
      (() => {
        let cached: Rrc<'album'> | undefined;
        const rrc = () => cached ?? (cached = randomRowAndColumn('album'));
        return {
          name: 'contradictory branches',
          createQuery: q => {
            const {randomRow, randomColumn} = rrc();
            return q.album.where(({or, cmp}) =>
              or(
                cmp(randomColumn, '=', randomRow[randomColumn] as any),
                cmp(randomColumn, '!=', randomRow[randomColumn] as any),
              ),
            );
          },
        };
      })(),
      // or paired with exists
      (() => {
        let cached: Rrc<'invoice'> | undefined;
        const rrc = () => cached ?? (cached = randomRowAndColumn('invoice'));
        return {
          name: 'exists in a branch',
          createQuery: q => {
            const {randomRow} = rrc();
            return q.invoice.where(({or, cmp, exists}) =>
              or(cmp('customerId', '=', randomRow.customerId), exists('lines')),
            );
          },
        };
      })(),
      // This is currently unsupported in z2s
      // test.each(tables.map(table => [table]))('0-branches %s', async table => {
      //   await checkZqlAndSql(
      //     pg,
      //     (zqliteQueries[table] as AnyQuery).where(({or}) => or()),
      //     (memoryQueries[table] as AnyQuery).where(({or}) => or()),
      //   );
      // });
    ],
  ),
)('$name', async ({fn}) => {
  await fn();
});

function getRelationships(table: string) {
  return Object.keys(
    (schema.relationships as Record<string, Record<string, unknown>>)[table] ??
      {},
  );
}

function randomRowAndColumn<TTable extends keyof Schema['tables']>(
  table: TTable,
): {
  randomRow: PullRow<TTable, Schema>;
  randomColumn: keyof Schema['tables'][TTable]['columns'];
} {
  const rows = must(data!.get(table));
  const randomRow = rows[Math.floor(Math.random() * rows.length)] as PullRow<
    TTable,
    Schema
  >;
  const columns = Object.keys(randomRow);
  const columnIndex = Math.floor(Math.random() * columns.length);
  const randomColumn = columns[
    columnIndex
  ] as keyof Schema['tables'][TTable]['columns'];
  return {randomRow, randomColumn};
}

function randomOperator(): SimpleOperator {
  return operators[Math.floor(Math.random() * operators.length)];
}
