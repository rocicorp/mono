import {bench, describe} from '../../shared/src/bench.ts';
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
describe('tracks with artist name', () => {
  bench('flipped', async () => {
    await delegates.sqlite.run(
      queries.artist
        .where('name', 'AC/DC')
        .related('albums', a => a.related('tracks')),
    );
  });

  bench('not flipped', async () => {
    await delegates.sqlite.run(
      queries.track.whereExists('album', a =>
        a.whereExists('artist', ar => ar.where('name', 'AC/DC')),
      ),
    );
  });
});
