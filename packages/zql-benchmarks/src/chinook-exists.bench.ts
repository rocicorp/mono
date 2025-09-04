import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {bench, run, summary} from 'mitata';
import {expect, test} from 'vitest';

const pgContent = await getChinook();

const {queries} = await bootstrap({
  suiteName: 'chinook_bench_exists',
  zqlSchema: schema,
  pgContent,
});

// const zql = createQuery(queries.memory);
// const zqlite = createQuery(queries.sqlite);

// TODO: also need tests that these produce the same results.
// TODO: test mem source and sqlite source
summary(() => {
  bench('tracks with artist name : flipped', async () => {
    await queries.sqlite.track.whereExists('album', a =>
      a.whereExists('artist', ar => ar.where('name', 'AC/DC'), {root: true}),
    );
  });
  bench('tracks with artist name : not flipped', async () => {
    await queries.sqlite.track.whereExists('album', a =>
      a.whereExists('artist', ar => ar.where('name', 'AC/DC')),
    );
  });
});

summary(() => {
  bench('tracks with album title : flipped', async () => {
    await queries.sqlite.track.whereExists(
      'album',
      a => a.where('title', 'For Those About To Rock We Salute You'),
      {root: true},
    );
  });
  bench('tracks with album title : not flipped', async () => {
    await queries.sqlite.track.whereExists('album', a =>
      a.where('title', 'For Those About To Rock We Salute You'),
    );
  });
});

await run();

const flipResult = await queries.sqlite.track.whereExists(
  'album',
  a => a.where('title', 'For Those About To Rock We Salute You'),
  {root: true},
);

const noFlipResult = await queries.sqlite.track.whereExists('album', a =>
  a.where('title', 'For Those About To Rock We Salute You'),
);

console.log(JSON.stringify(flipResult, null, 2));
console.log(JSON.stringify(noFlipResult, null, 2));

test('noop', () => {
  expect(true).toBe(true);
});
