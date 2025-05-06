import {runHydrationBenchmarks} from '../../zql-integration-tests/src/helpers/runner.ts';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';

const pgContent = await getChinook();

await runHydrationBenchmarks(
  {
    suiteName: 'chinook_bench',
    pgContent,
    zqlSchema: schema,
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
        q.playlist.related('tracks', t =>
          t
            .related('mediaType')
            .related('genre')
            .related('album', a => a.related('artist')),
        ),
    },
  ],
);
