// oxlint-disable valid-describe-callback
// oxlint-disable expect-expect
/* oxlint-disable @typescript-eslint/no-explicit-any */
import {describe, test} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import type {SimpleOperator} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {AnyQuery, PullRow} from '../../../zql/src/query/query.ts';
import {createVitests} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

// Junction edges do not correctly handle limits in ZQL
// oxlint-disable-next-line unicorn/prefer-set-has -- Keep as array for consistency with existing code
const brokenRelationshipLimits = ['tracks', 'customer', 'playlists'];

const pgContent = await getChinook();
const tables = Object.keys(schema.tables) as Array<keyof typeof schema.tables>;
let data: ReadonlyMap<string, readonly Row[]> | undefined;
type Schema = typeof schema;
type Rrc<T extends keyof Schema['tables']> = ReturnType<
  typeof randomRowAndColumn<T>
>;
const operators = ['=', '!=', '>', '>=', '<', '<='] as const;

describe(
  'Chinook PG Tests',
  {
    timeout: 30_000,
  },
  async () => {
    test.each(
      await createVitests(
        {
          suiteName: 'compiler_chinook',
          pgContent,
          zqlSchema: schema,
          setRawData: r => {
            data = r;
          },
        },
        [
          {
            name: 'compare primary key',
            createQuery: q => q.track.where('id', '=', 2941),
            manualVerification: [
              {
                albumId: 233,
                bytes: 9800861,
                composer: 'Adam Clayton, Bono, Larry Mullen, The Edge',
                genreId: 1,
                id: 2941,
                mediaTypeId: 1,
                milliseconds: 296280,
                name: 'Walk On',
                unitPrice: 0.99,
              },
            ],
          },
          {
            name: 'compare primary key and use `one`',
            createQuery: q => q.track.where('id', '=', 2941).one(),
            manualVerification: {
              albumId: 233,
              bytes: 9800861,
              composer: 'Adam Clayton, Bono, Larry Mullen, The Edge',
              genreId: 1,
              id: 2941,
              mediaTypeId: 1,
              milliseconds: 296280,
              name: 'Walk On',
              unitPrice: 0.99,
            },
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
                c.related('supportRep', r => r.related('reportsToEmployee')),
              ),
          },
          {
            name: 'Build a playlist',
            createQuery: q =>
              q.playlist
                .related('tracks', t =>
                  t
                    .related('mediaType')
                    .related('genre')
                    .related('album', a => a.related('artist')),
                )
                .limit(2),
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
            createQuery: q =>
              q.track.where(({exists}) => exists('invoiceLines')),
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
            name: 'Scalar subquery: tracks for album by title',
            createQuery: q =>
              q.track.where(({cmp, scalar}) =>
                cmp(
                  'albumId',
                  scalar(q.album.where('title', 'Riot Act').one(), 'id'),
                ),
              ),
          },
          // Note: != scalar subquery is not tested here because it rewrites to
          // NOT EXISTS which is not supported on the memory backend.
          {
            name: 'Scalar subquery: with and combinator',
            createQuery: q =>
              q.track.where(({and, cmp, scalar}) =>
                and(
                  cmp(
                    'albumId',
                    scalar(q.album.where('title', 'Riot Act').one(), 'id'),
                  ),
                  cmp('name', 'LIKE', '%Mine%'),
                ),
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
            createQuery: q =>
              q.album.where('title', 'NOT LIKE', '%Act').limit(1),
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
                  'Restless and Wild',
                  'For Those About To Rock We Salute You',
                ])
                .limit(10),
          },
          {
            name: 'Junction related with where',
            createQuery: q =>
              q.playlist
                .where('id', 17)
                .related('tracks', t => t.where('id', 1).one()),
            manualVerification: [
              {
                id: 17,
                name: 'Heavy Metal Classic',
                tracks: {
                  albumId: 1,
                  bytes: 11170334,
                  composer: 'Angus Young, Malcolm Young, Brian Johnson',
                  genreId: 1,
                  id: 1,
                  mediaTypeId: 1,
                  milliseconds: 343719,
                  name: 'For Those About To Rock (We Salute You)',
                  unitPrice: 0.99,
                },
              },
            ],
          },
          {
            name: 'related with pk condition',
            createQuery: q =>
              q.artist.related('albums', a => a.where('id', '=', 1)).one(),
            manualVerification: {
              albums: [
                {
                  artistId: 1,
                  id: 1,
                  title: 'For Those About To Rock We Salute You',
                },
              ],
              id: 1,
              name: 'AC/DC',
            },
          },

          {
            name: 'Junction related with where, no limits',
            createQuery: q =>
              q.playlist
                .where('id', 17)
                .related('tracks', t => t.where('id', 1)),
            manualVerification: [
              {
                id: 17,
                name: 'Heavy Metal Classic',
                tracks: [
                  {
                    albumId: 1,
                    bytes: 11170334,
                    composer: 'Angus Young, Malcolm Young, Brian Johnson',
                    genreId: 1,
                    id: 1,
                    mediaTypeId: 1,
                    milliseconds: 343719,
                    name: 'For Those About To Rock (We Salute You)',
                    unitPrice: 0.99,
                  },
                ],
              },
            ],
          },
          // zql and zqlite are currently unable to order over junction edges
          // {
          //   name: 'Junction related order by',
          //   createQuery: q =>
          //     q.playlist
          //       .where('id', 17)
          //       .related('tracks', t => t.orderBy('name', 'asc')),
          // },
          // zql and zqlite are currently unable to limit on junction edges
          // {
          //   name: 'Junction related order by with limit',
          //   createQuery: q =>
          //     q.playlist
          //       .where('id', 17)
          //       .related('tracks', t => t.orderBy('name', 'asc').limit(1)),
          // },
          {
            name: 'Junction related where on non primary key',
            createQuery: q =>
              q.playlist
                .where('id', 17)
                .related('tracks', t =>
                  t.where('name', 'For Those About To Rock'),
                ),
            manualVerification: [
              {
                id: 17,
                name: 'Heavy Metal Classic',
                tracks: [],
              },
            ],
          },
          {
            name: 'Permission check (via exists) against parent row',
            createQuery: q =>
              q.invoice
                .where('id', 1)
                .where('customerId', 2)
                .related('lines', t =>
                  t.whereExists('invoice', eb =>
                    eb.where('customerId', '=', 2),
                  ),
                ),
            manualVerification: [
              {
                billingAddress: 'Theodor-Heuss-Straße 34',
                billingCity: 'Stuttgart',
                billingCountry: 'Germany',
                billingPostalCode: '70174',
                billingState: null,
                customerId: 2,
                id: 1,
                invoiceDate: 1609459200000,
                lines: [
                  {
                    id: 1,
                    invoiceId: 1,
                    quantity: 1,
                    trackId: 2,
                    unitPrice: 0.99,
                  },
                  {
                    id: 2,
                    invoiceId: 1,
                    quantity: 1,
                    trackId: 4,
                    unitPrice: 0.99,
                  },
                ],
                total: 1.98,
              },
            ],
          },
        ],
        // primary key compare for each table
        (() =>
          tables.map(
            table =>
              ({
                name: `${table} pk lookup`,
                createQuery: q => {
                  const pk = schema.tables[table].primaryKey;
                  let ret = q[table] as AnyQuery;
                  for (const column of pk) {
                    ret = ret.where(column, '=', 1);
                  }
                  return ret;
                },
              }) as const,
          ))(),
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
                  createQuery: q =>
                    (q[table] as AnyQuery).related(relationship),
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
            const rrc = () =>
              cached ?? (cached = randomRowAndColumn('employee'));
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
              | {
                  rowsAndColumns: Array<Rrc<'artist'>>;
                  operators: SimpleOperator[];
                }
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
            const rrc = () =>
              cached ?? (cached = randomRowAndColumn('invoice'));
            return {
              name: 'exists in a branch',
              createQuery: q => {
                const {randomRow} = rrc();
                return q.invoice.where(({or, cmp, exists}) =>
                  or(
                    cmp('customerId', '=', randomRow.customerId),
                    exists('lines'),
                  ),
                );
              },
            };
          })(),
          // ordering by nullable columns
          {
            name: 'Order by nullable column',
            createQuery: q => q.customer.orderBy('company', 'desc').limit(10),
          },
          {
            name: 'Order by column (single row)',
            createQuery: q => q.employee.orderBy('firstName', 'asc').limit(1),
          },
          {
            name: 'Order by nullable column (single row)',
            createQuery: q => q.employee.orderBy('reportsTo', 'asc').limit(1),
          },
          {
            name: 'Order by nullable column',
            createQuery: q => q.employee.orderBy('reportsTo', 'desc').limit(6),
          },
          {
            name: 'Order by nullable column with where (is not null)',
            createQuery: q =>
              q.employee
                .where('reportsTo', 'IS NOT', null)
                .orderBy('reportsTo', 'desc')
                .limit(6),
          },
          {
            name: 'Order by nullable column with where (is null)',
            createQuery: q =>
              q.customer
                .where('company', 'IS', null)
                .orderBy('company', 'desc')
                .limit(10),
          },
          {
            name: 'Order by nullable column with where inequality',
            createQuery: q => q.employee.where('reportsTo', '<', 1).limit(6),
          },
          {
            name: 'Order by nullable column with where inequality',
            createQuery: q => q.employee.where('reportsTo', '<=', 6).limit(6),
          },
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
  },
);

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
