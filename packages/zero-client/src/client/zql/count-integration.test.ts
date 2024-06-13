import {describe, expect, test} from 'vitest';
import * as agg from 'zql/src/zql/query/agg.js';
import {exp, or} from 'zql/src/zql/query/entity-query.js';
import {
  createRandomAlbums,
  createRandomArtists,
  createRandomTracks,
  newZero,
} from './integration-test-util.js';

describe('count', async () => {
  const z = newZero();
  const artists = createRandomArtists(10);
  const albums = createRandomAlbums(10, artists);
  const tracks = createRandomTracks(100, albums);

  for (const artist of artists) {
    await z.mutate.artist.create(artist);
  }
  for (const album of albums) {
    await z.mutate.album.create(album);
  }
  for (const track of tracks) {
    await z.mutate.track.create(track);
  }

  test.each([
    {
      test: 'count full table',
      zql: z.query.artist.select(agg.count()),
      expected: [artists.length],
    },
    {
      test: 'count with where',
      zql: z.query.artist.select(agg.count()).where(
        'name',
        'IN',
        artists.slice(0, 5).map(a => a.name),
      ),
      expected: [5],
    },
    {
      test: 'count with or',
      zql: z.query.artist
        .select(agg.count())
        .where(
          or(
            exp('name', '=', artists[0].name),
            exp('name', '=', artists[1].name),
          ),
        ),
      expected: [2],
    },
    {
      test: 'count with group by',
      zql: z.query.artist.select(agg.count()).groupBy('name'),
      expected: artists.map(() => 1),
    },
    {
      test: 'count with fk join',
      zql: z.query.track
        .select(agg.count())
        .join(z.query.album, 'album', 'track.albumId', 'album.id')
        .select(agg.count()),
      expected: [tracks.length],
    },
  ])('$test', async ({zql, expected}) => {
    const stmt = zql.prepare();
    const rows = await stmt.exec();
    stmt.destroy();

    expect(rows.map(r => r.count)).toEqual(expected);
  });
});
