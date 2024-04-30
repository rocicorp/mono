import {describe, expect, test} from 'vitest';
import {joinSymbol} from '../../../../zql/src/zql/ivm/types.js';
import {newZero, Track, TrackArtist} from './integration-test-util.js';

describe('distinct', async () => {
  const z = newZero();
  const artists = [
    {
      id: '1',
      name: 'a',
    },
    {
      id: '2',
      name: 'b',
    },
    {
      id: '3',
      name: 'c',
    },
  ];
  const tracks: Track[] = [
    {
      id: '1',
      length: 100,
      title: 'a',
      albumId: '1',
    },
    {
      id: '2',
      length: 200,
      title: 'b',
      albumId: '1',
    },
    {
      id: '3',
      length: 300,
      title: 'c',
      albumId: '1',
    },
  ];
  const trackArtists: TrackArtist[] = [
    {
      id: '1-1',
      artistId: '1',
      trackId: '1',
    },
    {
      id: '2-1',
      artistId: '2',
      trackId: '1',
    },
    {
      id: '1-2',
      artistId: '1',
      trackId: '2',
    },
    {
      id: '2-2',
      artistId: '2',
      trackId: '2',
    },
    {
      id: '1-3',
      artistId: '1',
      trackId: '3',
    },
    {
      id: '2-3',
      artistId: '2',
      trackId: '3',
    },
  ];

  for (const artist of artists) {
    await z.mutate.artist.create(artist);
  }
  for (const track of tracks) {
    await z.mutate.track.create(track);
  }
  for (const trackArtist of trackArtists) {
    await z.mutate.trackArtist.create(trackArtist);
  }
  await new Promise(r => setTimeout(r, 100));

  test.each([
    {
      test: 'distinct on unique col against full table',
      zql: z.query.artist.distinct('artist.id'),
      expected: artists,
    },
    {
      test: 'distinct on non-unique col against full table',
      zql: z.query.track.distinct('track.albumId'),
      expected: [tracks[0]],
    },
    {
      test: 'distinct after 1:many join',
      zql: z.query.track
        .join(
          z.query.trackArtist,
          'trackArtist',
          'track.id',
          'trackArtist.trackId',
        )
        .distinct('track.id'),
      expected: [
        {
          id: '1-2_2',
          track: {id: '2', length: 200, title: 'b', albumId: '1'},
          trackArtist: {id: '1-2', artistId: '1', trackId: '2'},
          [joinSymbol]: true,
        },
        {
          id: '1-3_3',
          track: {id: '3', length: 300, title: 'c', albumId: '1'},
          trackArtist: {id: '1-3', artistId: '1', trackId: '3'},
          [joinSymbol]: true,
        },
        {
          id: '1_1-1',
          track: {id: '1', length: 100, title: 'a', albumId: '1'},
          trackArtist: {id: '1-1', artistId: '1', trackId: '1'},
          [joinSymbol]: true,
        },
      ],
    },
    {
      test: 'distinct after 1:many join thru junction edge',
      zql: z.query.track
        .join(
          z.query.trackArtist,
          'trackArtist',
          'track.id',
          'trackArtist.trackId',
        )
        .join(z.query.artist, 'artist', 'trackArtist.artistId', 'artist.id')
        .distinct('track.id'),
      expected: [
        {
          id: '1_1-2_2',
          track: {id: '2', length: 200, title: 'b', albumId: '1'},
          trackArtist: {id: '1-2', artistId: '1', trackId: '2'},
          artist: {id: '1', name: 'a'},
          [joinSymbol]: true,
        },
        {
          id: '1_1-3_3',
          track: {id: '3', length: 300, title: 'c', albumId: '1'},
          trackArtist: {id: '1-3', artistId: '1', trackId: '3'},
          artist: {id: '1', name: 'a'},
          [joinSymbol]: true,
        },
        {
          id: '1_1_1-1',
          track: {id: '1', length: 100, title: 'a', albumId: '1'},
          trackArtist: {id: '1-1', artistId: '1', trackId: '1'},
          artist: {id: '1', name: 'a'},
          [joinSymbol]: true,
        },
      ],
    },
  ])('$test', async ({zql, expected}) => {
    const stmt = zql.prepare();
    const rows = await stmt.exec();
    stmt.destroy();

    expect(rows).toEqual(expected);
  });
});
