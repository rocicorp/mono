/**
 * Regression tests for the push-overlay / `start` interaction in `MemorySource`.
 *
 * `MemorySource.#fetch` passed `req.start.row` to `generateWithOverlay` as the
 * stream's lower bound, and `overlaysForStartAt` pruned the in-flight push
 * overlay with the **index** comparator. That is only sound when the index sort
 * *is* the connection sort — i.e. when there is no constraint, so `scanStart`
 * really is `req.start.row`. With a constraint the index leads with the
 * constraint keys, so the comparison measured the wrong thing and dropped a
 * valid overlay, and the fetch returned pre-push data.
 *
 * A flipped EXISTS is what puts the two together: `FlippedJoin.#fetchBatched`
 * fetches its parent with `multiConstraints` (which `#fetchMulti` splits into
 * per-value sub-fetches, each carrying a primary-key `constraint`) while
 * forwarding the `start` that `Take` uses for its maintenance fetches. `Take`
 * then reads a stale stream mid-push and either asserts or silently drops rows.
 *
 * Both cases below were found by the chinook backbone fuzzer
 * (`checkYieldPush`), but neither needs the `'yield'` machinery — they are
 * plain push-maintenance bugs. `TableSource` (SQLite) was never affected;
 * these run under both source implementations to pin that they agree.
 */
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {makeSourceChangeAdd, makeSourceChangeEdit} from '../ivm/source.ts';
import type {Source} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';

const lc = createSilentLogContext();

// ── Case A: the overlay is invisible, so Take cannot find the row after its bound ──

const genre = table('genre')
  .columns({id: number(), name: string()})
  .primaryKey('id');
const track = table('track')
  .columns({id: number(), genreId: number()})
  .primaryKey('id');

const genreSchema = createSchema({
  tables: [genre, track],
  relationships: [
    relationships(genre, ({many}) => ({
      tracks: many({
        sourceField: ['id'],
        destField: ['genreId'],
        destSchema: track,
      }),
    })),
  ],
});

test('flipped exists + take: editing a row past the bound keeps the window correct', () => {
  const sources: Record<string, Source> = {
    genre: createSource(
      lc,
      testLogConfig,
      'genre',
      genreSchema.tables.genre.columns,
      genreSchema.tables.genre.primaryKey,
    ),
    track: createSource(
      lc,
      testLogConfig,
      'track',
      genreSchema.tables.track.columns,
      genreSchema.tables.track.primaryKey,
    ),
  };
  const g = must(sources.genre);
  const t = must(sources.track);
  for (const row of [
    {id: 1, name: 'Rock'},
    {id: 2, name: 'Jazz'},
    {id: 3, name: 'Metal'},
    {id: 4, name: 'Empty'}, // no tracks -- filtered out by the gate
  ] as Row[]) {
    consume(g.push(makeSourceChangeAdd(row)));
  }
  for (const row of [
    {id: 100, genreId: 1},
    {id: 102, genreId: 2},
    {id: 103, genreId: 3},
  ] as Row[]) {
    consume(t.push(makeSourceChangeAdd(row)));
  }
  const delegate = new QueryDelegateImpl({sources});

  // `limit` >= the number of matching rows, so the take window is never full
  // and its bound is the largest row. That is what drives the edit below into
  // Take's "old inside, new past the bound" arm, whose maintenance fetch
  // carries a `start`.
  const view = delegate.materialize(
    newQuery(genreSchema, 'genre')
      .where(({exists}) => exists('tracks', undefined, {flip: true}))
      .orderBy('name', 'asc')
      .orderBy('id', 'desc')
      .limit(10),
  );
  expect(view.data.map(r => r.name)).toEqual(['Jazz', 'Metal', 'Rock']);

  // Rock(1) is the bound. Move it to the front; Metal(3) becomes the bound.
  consume(
    g.push(makeSourceChangeEdit({id: 1, name: 'AAA'}, {id: 1, name: 'Rock'})),
  );
  expect(view.data.map(r => r.name)).toEqual(['AAA', 'Jazz', 'Metal']);

  // Move it back past the bound. Take fetches for the row after Metal(3); that
  // row is the edit itself, visible only through the push overlay. Before the
  // fix the overlay was pruned and this threw
  // `Take: afterBoundNode must be found during fetch`.
  consume(
    g.push(makeSourceChangeEdit({id: 1, name: 'Rock'}, {id: 1, name: 'AAA'})),
  );
  expect(view.data.map(r => r.name)).toEqual(['Jazz', 'Metal', 'Rock']);
});

// ── Case B: same defect, silent -- a row that should enter the window does not ──

const employee = table('employee')
  .columns({id: number(), name: string(), reportsTo: number().optional()})
  .primaryKey('id');

const employeeSchema = createSchema({
  tables: [employee],
  relationships: [
    relationships(employee, ({one}) => ({
      manager: one({
        sourceField: ['reportsTo'],
        destField: ['id'],
        destSchema: employee,
      }),
    })),
  ],
});

test('flipped self-join exists in an OR, with start + limit: an edit adds the row', () => {
  const sources: Record<string, Source> = {
    employee: createSource(
      lc,
      testLogConfig,
      'employee',
      employeeSchema.tables.employee.columns,
      employeeSchema.tables.employee.primaryKey,
    ),
  };
  const e = must(sources.employee);
  for (const row of [
    {id: 1, name: 'Adams', reportsTo: null},
    {id: 2, name: 'Mills', reportsTo: 1},
    {id: 3, name: 'Park', reportsTo: 2},
  ] as Row[]) {
    consume(e.push(makeSourceChangeAdd(row)));
  }
  const delegate = new QueryDelegateImpl({sources});

  const view = delegate.materialize(
    newQuery(employeeSchema, 'employee')
      .where(({or, cmp, exists}) =>
        or(exists('manager', undefined, {flip: true}), cmp('id', '=', 2)),
      )
      .orderBy('reportsTo', 'desc')
      .start({id: 3, reportsTo: 2}, {inclusive: false})
      .limit(2),
  );
  expect(view.data.map(r => r.id)).toEqual([2]);

  // Adams starts reporting to itself, so it now passes the flipped gate and
  // sorts into the window. Before the fix the push overlay was invisible to the
  // constrained maintenance fetch and Adams was silently never added.
  consume(
    e.push(
      makeSourceChangeEdit(
        {id: 1, name: 'Adams', reportsTo: 1},
        {id: 1, name: 'Adams', reportsTo: null},
      ),
    ),
  );
  expect(view.data.map(r => r.id)).toEqual([1, 2]);
});
