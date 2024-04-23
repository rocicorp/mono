import {expect, test} from 'vitest';
import {Track, setup, Artist, TrackArtist} from '../benchmarks/setup.js';
import * as agg from '../query/agg.js';

test('having against scalars', async () => {
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

  const stmt = trackQuery
    .select(agg.sum('length'))
    .having('length', '>', 50)
    .prepare();

  const rows = await stmt.exec();
  expect(rows).toEqual([
    {id: '1', title: 'Track 1', length: 300, albumId: '1'},
  ]);
  stmt.destroy();

  const stmt2 = trackQuery
    .select(agg.sum('length'))
    .having('length', '>', 300)
    .prepare();

  const rows2 = await stmt2.exec();
  expect(rows2).toEqual([]);
  stmt2.destroy();

  const stmt3 = trackQuery
    .select(agg.sum('length'))
    .having('length', '>=', 300)
    .prepare();

  const rows3 = await stmt3.exec();
  expect(rows3).toEqual([
    {id: '1', title: 'Track 1', length: 300, albumId: '1'},
  ]);
  stmt3.destroy();
});

test('having against arrays / sets', async () => {
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

  const queryPiece = trackQuery
    .join(trackArtistQuery, 'trackArtists', 'track.id', 'trackArtist.trackId')
    .join(artistQuery, 'artists', 'trackArtists.artistId', 'artist.id')
    .groupBy('track.id');

  const stmt = queryPiece
    .select('track.id', 'track.title', agg.array('artists.name', 'artists'))
    .having('artists', 'CONGRUENT', ['Artist 1'])
    .prepare();

  // TODO:
  // aggArray hoists the selected field out
  // so... we need to update the type to reflect that in TS.

  const rows = await stmt.exec();

  console.log(rows);

  // aggArray(artists.name) -> does not seem to create an array of names but rather an array of rows?
  // or at least the type system has it wrong.
  // can we count artists in the group?
  // having against a propery of a thing in an array should lift it to a new array?
  // what if it is `having(artists.name, =, foo)`? This is a non-set operation.
  // this is an inspection of 1 element matching foo.
  // maybe should force set operations
});
