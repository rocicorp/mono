import {bench, run, summary} from 'mitata';
import {expect, test} from 'vitest';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';

const pgContent = await getChinook();

const {queries, delegates} = await bootstrap({
  suiteName: 'chinook_bench_exists',
  zqlSchema: schema,
  pgContent,
});

// Demonstration of how to compare two different query styles
summary(() => {
  bench('tracks with artist name : flipped', async () => {
    await delegates.sqlite.run(
      queries.artist
        .where('name', 'AC/DC')
        .related('albums', a => a.related('tracks')),
    );
  });

  bench('tracks with artist name : not flipped', async () => {
    await delegates.sqlite.run(
      queries.track.whereExists('album', a =>
        a.whereExists('artist', ar => ar.where('name', 'AC/DC')),
      ),
    );
  });
});

await run();

// here so we can run with a vitest and get all the pg setup goodness
test('noop', () => {
  expect(true).toBe(true);
});
