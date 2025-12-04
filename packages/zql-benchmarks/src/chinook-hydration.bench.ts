import {runBenchmarks} from '../../zql-integration-tests/src/helpers/runner.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {expect, test} from 'vitest';

const pgContent = await getChinook();

await runBenchmarks(
  {
    suiteName: 'chinook_bench_hydrate',
    type: 'hydration',
    pgContent,
    zqlSchema: schema,
    only: 'all playlists',
  },
  [
    {
      name: '(table scan) select * from album',
      createQuery: q => q.album,
    },
    {
      name: '(pk lookup) select * from track where id = 3163',
      createQuery: q => q.track.where('id', 3163),
    },
    {
      name: '(secondary index lookup) select * from track where album_id = 248',
      createQuery: q => q.track.where('albumId', 248),
    },
    {
      name: 'scan with one depth related',
      createQuery: q => q.album.related('artist'),
    },
    {
      name: 'all playlists',
      createQuery: q =>
        q.playlist.related(
          'tracks',
          t => t,
          // .related('mediaType')
          // .related('genre')
          // .related('album', a => a.related('artist')),
        ),
    },
    {
      name: 'OR with empty branch and limit',
      createQuery: q =>
        q.track
          .where(({or, cmp}) =>
            or(cmp('milliseconds', -1), cmp('mediaTypeId', 1)),
          )
          .limit(5),
    },
    {
      name: 'OR with empty branch and limit with exists',
      createQuery: q =>
        q.track
          .where(({or, cmp, exists}) =>
            or(
              cmp('milliseconds', -1),
              exists('mediaType', q => q.where('id', 1)),
            ),
          )
          .limit(5),
    },
  ],
);

test('no-op', () => {
  expect(true).toBe(true);
});
