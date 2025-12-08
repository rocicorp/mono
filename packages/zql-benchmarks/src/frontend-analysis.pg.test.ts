/* oxlint-disable no-console */
import {B, do_not_optimize, type trial} from 'mitata';
import {expect, test} from 'vitest';
import type {JSONValue} from '../../shared/src/json.ts';
import {must} from '../../shared/src/must.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import type {AnyQueryBuilder} from '../../zql/src/query/query-builder.ts';

const pgContent = await getChinook();

const harness = await bootstrap({
  suiteName: 'frontend_analysis',
  zqlSchema: schema,
  pgContent,
});
const edit = makeEdit();

log`
# Point Queries

A UI may:
1. Need to run many point queries at once (e.g. for a list of items)
2. Have many point queries open at once (e.g. same list of items)

We need to understand the limits of both cases.
- How many point queries can we hydrate at once?
- How many writes per second can we do with many point queries open at once?`;

log`
## Hydrate 100 point queries

How long to hydrate 100 point queries?
100 seems like a reasonable number. The intuition here is that this
could be a few pages of a list or table view.
1,000 point queries could be possible if each cell in a table grabbed its own data.`;

const hydrate100PointQueries = new B('hydrate 100 point queries', function* () {
  yield async () => {
    await Promise.all(
      Array.from({length: 100}, (_, i) =>
        harness.queries.track.where('id', i + 1),
      ),
    );
  };
});

const ptTrial = await hydrate100PointQueries.run();
const ptMs = p99(ptTrial);
log`${frameStats(ptTrial)}`;

log`
### Findings

${stat(ptMs / 16)}  frames to hydrate 100 point queries which is
~${stat(1 / (ptMs / 16))} fps.

Is this reasonable? Lets check against point lookups from a map.
`;

const pointLookupFromMap = new B('point lookup from map', function* () {
  yield () =>
    Array.from({length: 100}, (_, i) =>
      do_not_optimize(harness.dbs.raw.get('track')![i]),
    );
});
const mapTrial = await pointLookupFromMap.run();
const mapMs = p99(mapTrial);
log`
- Point lookup from map: ${stat(mapMs, 10)}ms.
- Frame budget: 16ms
- Total frames: ${stat(mapMs / 16, 10)}
- FPS: ${stat(1 / (mapMs / 16))}`;

log`
Point lookup perf is entirely **unreasonable**.
- We can only hydrate 100 points queries at ${stat(1 / (ptMs / 16))}fps
- We can do 100 point lookups from a map at ${stat(1 / (mapMs / 16))}fps
`;

log`
## Maintain 100 Point Queries
How many writes per second can we do with 100 point queries open at once?
`;

const maintain100PointQueries = new B(
  'maintain 100 point queries',
  function* () {
    const views = Array.from({length: 100}, (_, i) =>
      harness.delegates.memory.materialize(
        harness.queries.track.where('id', i + 1),
      ),
    );
    let count = 0;
    yield () => {
      harness.delegates.memory
        .getSource('track')!
        .push(edit('track', count % 100, 'name', `new name ${count}`));
      count++;
    };
    views.forEach(view => {
      view.destroy();
    });
  },
);
const maintain100PointTrial = await maintain100PointQueries.run();
log`
${frameMaintStats(maintain100PointTrial)}`;

log`
# Range Queries

Single table range queries may or may not be common, depending on how the user structures their app.
A user may do everything as range queries if queries are colocated with components thus pushing the
"join" into the UI.

10 range queries seems like a reasonable number for an app to have open at once.
- A list of tracks
- A list of albums
- A list of artists
- A list of playlists
- A list of liked things
- A list of genres
etc.

The limit on each range query would vary from 10 -> 100.
100 for a main list view, ~10 for all the auxiliary lists.

## Hydrate 10 Range Queries
`;

const hydrate10RangeQueries = new B('hydrate 10 range queries', function* () {
  const zql = harness.queries;
  yield async () => {
    await Promise.all([
      zql.track.limit(100),
      zql.album.limit(10),
      zql.artist.limit(10),
      zql.playlist.limit(10),
      zql.mediaType.limit(10),
      zql.genre.limit(10),
      zql.invoice.limit(10),
      zql.invoiceLine.limit(10),
      zql.customer.limit(10),
      zql.employee.limit(10),
    ]);
  };
});

const rangeTrial = await hydrate10RangeQueries.run();
log`
${frameStats(rangeTrial)}

**Interestingly range query hydration is faster than point query hydration.**
Even if we scaled this up to 100 range queries it would still be faster?

Alternatives to test:
1. Range queries will not always start at the beginning of the table.
2. Range queries will not always be sorted by primary key

## Hydrate 10 Range Queries, Order By Alternate Fields`;

const hydrate10RangeQueriesAltField = new B(
  'hydrate 10 range queries by alternate fields',
  function* () {
    const zql = harness.queries;
    yield async () => {
      await Promise.all([
        zql.track.orderBy('name', 'asc').limit(100),
        zql.album.orderBy('title', 'asc').limit(10),
        zql.artist.orderBy('name', 'asc').limit(10),
        zql.playlist.orderBy('name', 'asc').limit(10),
        zql.mediaType.orderBy('name', 'asc').limit(10),
        zql.genre.orderBy('name', 'asc').limit(10),
        zql.invoice.orderBy('invoiceDate', 'desc').limit(10),
        zql.invoiceLine.orderBy('unitPrice', 'desc').limit(10),
        zql.customer.orderBy('lastName', 'asc').limit(10),
        zql.employee.orderBy('lastName', 'asc').limit(10),
      ]);
    };
  },
);

const rangeAltFieldTrial = await hydrate10RangeQueriesAltField.run();
log`
${frameStats(rangeAltFieldTrial)}

Surprisingly this is not much slower than order by primary key. That is likely
due to the cost of index creation being paid once up front.

Starting in the middle of a table could still be expensive. The feeling is that it must be
because point queries are so slow.

## Hydrate 10 Range Queries, Start in the Middle of the Table
`;

const hydrate10RangeQueriesMiddle = new B(
  'hydrate 10 range queries, start in the middle of the table',
  function* () {
    const zql = harness.queries;
    yield async () => {
      await Promise.all([
        zql.track.start(middle('track')).limit(100),
        zql.album.start(middle('album')).limit(10),
        zql.artist.start(middle('artist')).limit(10),
        zql.playlist.start(middle('playlist')).limit(10),
        zql.mediaType.start(middle('mediaType')).limit(10),
        zql.genre.start(middle('genre')).limit(10),
        zql.invoice.start(middle('invoice')).limit(10),
        zql.invoiceLine.start(middle('invoiceLine')).limit(10),
        zql.customer.start(middle('customer')).limit(10),
        zql.employee.start(middle('employee')).limit(10),
      ]);
    };
  },
);

const rangeMiddleTrial = await hydrate10RangeQueriesMiddle.run();
log`
${frameStats(rangeMiddleTrial)}

Something strange. This is still faster than point queries.
## Scale to 100 Range Queries (limit 20 each)
`;

const hydrate100RangeQueries = new B(
  'hydrate 100 range queries (limit 20 each)',
  function* () {
    const zql = harness.queries;
    yield async () => {
      await Promise.all(Array.from({length: 100}, _ => zql.track.limit(20)));
    };
  },
);
const range100Trial = await hydrate100RangeQueries.run();
log`
${frameStats(range100Trial)}

Now to check maintenance of range queries during writes.`;

const maintain10RangeQueriesAltField = new B(
  'hydrate 10 range queries by alternate fields',
  function* () {
    const zql = harness.queries;
    const tables = [
      'track',
      'album',
      'artist',
      'playlist',
      'mediaType',
      'genre',
      'customer',
      'employee',
    ] as const;
    const views = [
      zql.track.orderBy('name', 'asc').limit(100),
      zql.album.orderBy('title', 'asc').limit(10),
      zql.artist.orderBy('name', 'asc').limit(10),
      zql.playlist.orderBy('name', 'asc').limit(10),
      zql.mediaType.orderBy('name', 'asc').limit(10),
      zql.genre.orderBy('name', 'asc').limit(10),
      zql.invoice.orderBy('invoiceDate', 'desc').limit(10),
      zql.invoiceLine.orderBy('unitPrice', 'desc').limit(10),
      zql.customer.orderBy('lastName', 'asc').limit(10),
      zql.employee.orderBy('lastName', 'asc').limit(10),
    ].map(q => harness.delegates.memory.materialize(q as AnyQueryBuilder));
    let count = 0;
    yield () => {
      const table = tables[count % tables.length];
      const cols = schema.tables[table].columns;
      // TODO: update to edit a thing that is in range...
      harness.delegates.memory
        .getSource(table)!
        .push(
          edit(
            table,
            count % 8,
            'name' in cols ? 'name' : 'title' in cols ? 'title' : 'lastName',
            `new name ${count}`,
          ),
        );
      count++;
    };
    views.forEach(view => {
      view.destroy();
    });
  },
);
const maintainRangeAltFieldTrial = await maintain10RangeQueriesAltField.run();
log`
## Maintain 10 Range Queries, Order By Alternate Fields
${frameMaintStats(maintainRangeAltFieldTrial)}

Well this is surprisingly faster than maintaining point queries!!!
- p99 ${stat(p99(maintain100PointTrial) / p99(maintainRangeAltFieldTrial))}x faster than point queries
- avg ${stat(avg(maintain100PointTrial) / avg(maintainRangeAltFieldTrial))}x faster than point queries

The time is dominated by number of pipelines to visit?`;

function frameStats(trial: trial) {
  const p99ms = p99(trial);
  const avgms = avg(trial);
  return `
  - Total time: ${stat(p99ms)}p99, ${stat(avgms)}avg
  - Frame budget: 16ms
  - FPS: ${stat(1 / (p99ms / 16))}p99, ${stat(1 / (avgms / 16))}avg`;
}

function frameMaintStats(trial: trial) {
  const p99ms = p99(trial);
  const avgms = avg(trial);
  return `
  - Maintain queries over 1 write: ${stat(p99ms)}ms p99 ${stat(avgms)}ms avg.
  - Frame budget: 16ms
  - Writes per frame: ${stat(16 / p99ms, 10)}p99, ${stat(16 / avgms, 10)}avg`;
}

function p99(trial: trial) {
  const stats = must(trial.runs[0].stats);
  return nanosToMs(stats.p99);
}

function avg(trial: trial) {
  const stats = must(trial.runs[0].stats);
  return nanosToMs(stats.avg);
}

function nanosToMs(nanos: number) {
  return nanos / 1_000_000;
}

test('noop', () => expect(true).toBe(true));

function log(strings: TemplateStringsArray, ...values: unknown[]): void {
  let result = '';
  strings.forEach((string, i) => {
    result += string;
    if (i < values.length) {
      result += values[i];
    }
  });

  console.log(result);
}

function stat(s: number, precision = 2) {
  return (
    '`' +
    s.toLocaleString('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: precision,
    }) +
    '`'
  );
}

function makeEdit() {
  const currentValues = new Map<string, Row>();
  return (
    table: keyof (typeof schema)['tables'],
    index: number,
    column: string,
    value: JSONValue,
  ) => {
    const key = `${table}-${index}`;
    const dataset = must(harness.dbs.raw.get(table));
    const row = must(currentValues.get(key) ?? dataset[index]);
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

function middle(table: keyof (typeof schema)['tables']) {
  const dataset = must(harness.dbs.raw.get(table));
  return dataset[Math.floor(dataset.length / 2)];
}
