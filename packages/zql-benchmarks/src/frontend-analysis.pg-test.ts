/* eslint-disable no-console */
import {B, do_not_optimize, type trial} from 'mitata';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {expect, test} from 'vitest';
import {must} from '../../shared/src/must.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import type {JSONValue} from '../../shared/src/json.ts';

const pgContent = await getChinook();

const harness = await bootstrap({
  suiteName: 'frontend_analysis',
  zqlSchema: schema,
  pgContent,
});

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
      Array.from({length: 100}, (_, i) => {
        const query = harness.queries.memory.track.where('id', i + 1);
        return query.run();
      }),
    );
  };
});

const ptTrial = await hydrate100PointQueries.run();
const ptMs = p99(ptTrial);
log`
- Hydrate 100 point queries: \`${ptMs}\`ms.
- Frame budget: 16ms
- Total frames: ${stat(ptMs / 16)}
- FPS: ${stat(1 / (ptMs / 16))}`;

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
      harness.queries.memory.track.where('id', i + 1).materialize(),
    );
    const edit = makeEdit();
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
const maintainTrial = await maintain100PointQueries.run();
const maintainMs = p99(maintainTrial);
log`
- Maintain 100 point queries over 1 write: ${stat(maintainMs)}ms.
- Frame budget: 16ms
- Writes per frame: ${stat(16 / maintainMs, 10)}`;

function p99(trial: trial) {
  const stats = must(trial.runs[0].stats);
  return nanosToMs(stats.p99);
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
