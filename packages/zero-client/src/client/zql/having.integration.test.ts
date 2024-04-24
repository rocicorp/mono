import {describe, expect, test} from 'vitest';
import {Track, setup, Artist, TrackArtist} from '../benchmarks/setup.js';
import * as agg from '../query/agg.js';

describe('having against scalaer', async () => {
  const {r, trackQuery} = setup();

  const tracks: Track[] = [
    {
      id: '1',
      title: 'Track 1',
      length: 100,
      albumId: '1',
    },
    {
      id: '2',
      title: 'Track 2',
      length: 100,
      albumId: '1',
    },
    {
      id: '3',
      title: 'Track 3',
      length: 100,
      albumId: '1',
    },
  ];

  await r.mutate.bulkSet({tracks});
  test.each([
    [['>', 50], ['1']],
    [['>', 300], []],
    [['<', 300], []],
    [['<', 500], ['1']],
    [['=', 0], []],
    [['=', 300], ['1']],
  ] as const)('%j', async (input, expected) => {
    const stmt = trackQuery
      .select('id', agg.sum('length'))
      .having('length', input[0], input[1])
      .prepare();

    const rows = await stmt.exec();
    expect(rows.map(x => x.id)).toEqual(expected);
    stmt.destroy();
  });
});

describe('having against arrays / sets', async () => {
  const {r, trackArtistQuery, artistQuery, trackQuery} = setup();

  const tracks: Track[] = [
    {
      id: '1',
      title: 'Track 1',
      length: 100,
      albumId: '1',
    },
    {
      id: '2',
      title: 'Track 2',
      length: 100,
      albumId: '1',
    },
    {
      id: '3',
      title: 'Track 3',
      length: 100,
      albumId: '1',
    },
  ];
  const artists: Artist[] = [
    {
      id: '1',
      name: 'Artist 1',
    },
    {
      id: '2',
      name: 'Artist 2',
    },
    {
      id: '3',
      name: 'Artist 3',
    },
  ];
  const trackArtists: TrackArtist[] = [
    {
      id: '1-1',
      trackId: '1',
      artistId: '1',
    },
    {
      id: '2-2',
      trackId: '2',
      artistId: '2',
    },
    {
      id: '3-3',
      trackId: '3',
      artistId: '3',
    },
  ];

  await r.mutate.bulkSet({tracks, artists, trackArtists});

  test.each([
    [['CONGRUENT', ['Artist 1']], ['1']],
    [
      ['SUPERSET', []],
      ['1', '2', '3'],
    ],
    [['INTERSECTS', ['Artist 2']], ['2']],
    [
      ['INTERSECTS', ['Artist 1', 'Artist 2']],
      ['1', '2'],
    ],
    [['SUBSET', ['Artist 3']], ['3']],
    [
      ['DISJOINT', []],
      ['1', '2', '3'],
    ],
    [['DISJOINT', ['Artist 1', 'Artist 2']], ['3']],
    [
      ['INCONGRUENT', ['Artist 1', 'Artist 2']],
      ['1', '2', '3'],
    ],
  ] as const)('%j', async (input, expected) => {
    const stmt = trackQuery
      .join(trackArtistQuery, 'trackArtists', 'track.id', 'trackArtist.trackId')
      .join(artistQuery, 'artists', 'trackArtists.artistId', 'artist.id')
      .groupBy('track.id')
      .select('track.id', 'track.title', agg.array('artists.*', 'artists'))
      // TODO: `having` and `where` should mark their args readonly
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      .having('artists.name', input[0], input[1] as any)
      .prepare();

    const rows = await stmt.exec();
    expect(rows.map(r => r.track.id)).toEqual(expected);

    stmt.destroy();
  });
});
