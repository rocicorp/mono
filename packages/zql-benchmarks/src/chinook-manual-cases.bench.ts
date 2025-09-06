import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {bench, run, summary} from 'mitata';
import {expect, test} from 'vitest';
import {disableJoinStorage} from '../../zql/src/ivm/operator.ts';
import {disableImplicitLimitOne} from '../../zql/src/query/query-impl.ts';

const pgContent = await getChinook();

const {queries} = await bootstrap({
  suiteName: 'chinook_bench_exists',
  zqlSchema: schema,
  pgContent,
});

// Demonstration of how to compare two different query styles
summary(() => {
  bench('tracks with artist name : flipped', async () => {
    await queries.sqlite.artist
      .where('name', 'AC/DC')
      .related('albums', a => a.related('tracks'));
  });

  bench('tracks with artist name : not flipped', async () => {
    await queries.sqlite.track.whereExists('album', a =>
      a.whereExists('artist', ar => ar.where('name', 'AC/DC')),
    );
  });
});

summary(() => {
  bench('tracks with join storage on (sqlite)', async () => {
    disableJoinStorage.value = false;

    await queries.sqlite.track
      .related('album')
      .related('genre')
      .related('mediaType')
      .related('playlists');
  });

  bench('playlist with join storage off (sqlite)', async () => {
    disableJoinStorage.value = true;

    await queries.sqlite.track
      .related('album')
      .related('genre')
      .related('mediaType')
      .related('playlists');
  });
});

summary(() => {
  bench('no implicit limit 1', async () => {
    disableImplicitLimitOne.value = true;

    await queries.sqlite.track
      .related('album')
      .related('genre')
      .related('mediaType')
      .related('invoiceLines');
  });

  bench('always limit 1', async () => {
    disableImplicitLimitOne.value = false;

    await queries.sqlite.track
      .related('album', q => q.limit(1))
      .related('genre', q => q.limit(1))
      .related('mediaType', q => q.limit(1))
      .related('invoiceLines', q => q.limit(1));
  });
});

await run();

// here so we can run with a vitest and get all the pg setup goodness
test('noop', () => {
  expect(true).toBe(true);
});
