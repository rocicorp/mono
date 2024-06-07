import {joinSymbol} from '@rocicorp/zql/src/zql/ivm/types.js';
import * as agg from '@rocicorp/zql/src/zql/query/agg.js';
import {expect, test} from 'vitest';
import {
  Album,
  Artist,
  Playlist,
  Track,
  TrackArtist,
  bulkRemove,
  bulkSet,
  createRandomAlbums,
  createRandomArtists,
  createRandomTracks,
  linkTracksToArtists,
  newZero,
} from './integration-test-util.js';

test('direct foreign key join: join a track to an album', async () => {
  const z = newZero();

  const track: Track = {
    id: '1',
    title: 'Track 1',
    length: 100,
    albumId: '1',
  };
  const album: Album = {
    id: '1',
    title: 'Album 1',
    artistId: '1',
  };

  await Promise.all([
    z.mutate.track.create(track),
    z.mutate.album.create(album),
  ]);
  await Promise.resolve();

  const stmt = z.query.track
    .join(z.query.album, 'album', 'albumId', 'id')
    .select('*')
    .prepare();

  let rows = await stmt.exec();

  expect(rows).toEqual([
    {
      id: '1_1',
      track,
      album,
      [joinSymbol]: true,
    },
  ]);

  // delete the track
  await z.mutate.track.delete({id: track.id});

  rows = await stmt.exec();
  expect(rows).toEqual([]);

  // re-add a track for that album
  await z.mutate.track.create({
    id: '2',
    title: 'Track 1',
    length: 100,
    albumId: '1',
  });

  rows = await stmt.exec();
  const track2Album1 = {
    id: '1_2',
    track: {
      id: '2',
      title: 'Track 1',
      length: 100,
      albumId: '1',
    },
    album: {
      id: '1',
      title: 'Album 1',
      artistId: '1',
    },
    [joinSymbol]: true,
  };
  expect(rows).toEqual([track2Album1]);

  // add an unrelated album
  await z.mutate.album.create({
    id: '2',
    title: 'Album 2',
    artistId: '1',
  });

  rows = await stmt.exec();
  expect(rows).toEqual([track2Album1]);

  // add an unrelated track
  await z.mutate.track.create({
    id: '3',
    title: 'Track 3',
    length: 100,
    albumId: '3',
  });

  rows = await stmt.exec();
  expect(rows).toEqual([track2Album1]);

  // add an album related to track3
  await z.mutate.album.create({
    id: '3',
    title: 'Album 3',
    artistId: '1',
  });

  rows = await stmt.exec();
  const track3Album3 = {
    id: '3_3',
    track: {
      id: '3',
      title: 'Track 3',
      length: 100,
      albumId: '3',
    },
    album: {
      id: '3',
      title: 'Album 3',
      artistId: '1',
    },
    [joinSymbol]: true,
  };
  expect(rows).toEqual([track2Album1, track3Album3]);

  // add a track related to album2
  await z.mutate.track.create({
    id: '4',
    title: 'Track 4',
    length: 100,
    albumId: '2',
  });

  rows = await stmt.exec();
  const track4Album2 = {
    id: '2_4',
    track: {
      id: '4',
      title: 'Track 4',
      length: 100,
      albumId: '2',
    },
    album: {
      id: '2',
      title: 'Album 2',
      artistId: '1',
    },
    [joinSymbol]: true,
  };
  expect(rows).toEqual([track2Album1, track3Album3, track4Album2]);

  // add a second track to album 1
  await z.mutate.track.create({
    id: '5',
    title: 'Track 5',
    length: 100,
    albumId: '1',
  });

  rows = await stmt.exec();
  const track5Album1 = {
    id: '1_5',
    track: {
      id: '5',
      title: 'Track 5',
      length: 100,
      albumId: '1',
    },
    album: {
      id: '1',
      title: 'Album 1',
      artistId: '1',
    },
    [joinSymbol]: true,
  };
  expect(rows).toEqual([
    track2Album1,
    track3Album3,
    track4Album2,
    track5Album1,
  ]);

  // sort by track id
  const stmt2 = await z.query.track
    .join(z.query.album, 'album', 'albumId', 'id')
    .select('*')
    .orderBy('track.id', 'asc')
    .prepare();

  rows = await stmt2.exec();
  expect(rows).toEqual([
    track2Album1,
    track3Album3,
    track4Album2,
    track5Album1,
  ]);

  // delete all the things
  await Promise.all([
    z.mutate.track.delete({id: '2'}),
    z.mutate.track.delete({id: '3'}),
    z.mutate.track.delete({id: '4'}),
    z.mutate.album.delete({id: '1'}),
    z.mutate.album.delete({id: '2'}),
    z.mutate.album.delete({id: '3'}),
  ]);

  rows = await stmt.exec();
  expect(rows).toEqual([]);

  await z.close();
});

/**
 * A playlist has tracks.
 * Tracks should join in their albums and artists.
 * Artists should be aggregated into an array of artists for a given track, resulting in a single
 * row per track.
 */
test('junction and foreign key join, followed by aggregation: compose a playlist via a join and group by', async () => {
  const z = newZero();
  const track1: Track = {
    id: '1',
    title: 'Track 1',
    length: 100,
    albumId: '1',
  };
  const track2: Track = {
    id: '2',
    title: 'Track 2',
    length: 100,
    albumId: '1',
  };
  const track3: Track = {
    id: '3',
    title: 'Track 3',
    length: 100,
    albumId: '2',
  };
  const track4: Track = {
    id: '4',
    title: 'Track 4',
    length: 100,
    albumId: '2',
  };
  const tracks = [track1, track2, track3, track4];

  const album1: Album = {
    id: '1',
    title: 'Album 1',
    artistId: '1',
  };
  const album2: Album = {
    id: '2',
    title: 'Album 2',
    artistId: '1',
  };
  const albums = [album1, album2];

  const artist1: Artist = {
    id: '1',
    name: 'Artist 1',
  };
  const artist2: Artist = {
    id: '2',
    name: 'Artist 2',
  };
  const artist3: Artist = {
    id: '3',
    name: 'Artist 3',
  };
  const artists = [artist1, artist2, artist3];

  const playlist: Playlist = {
    id: '1',
    name: 'Playlist 1',
  };
  const playlist2: Playlist = {
    id: '2',
    name: 'Playlist 2',
  };
  const playlists = [playlist, playlist2];

  const playlistTracks = [
    {
      id: '1-1',
      playlistId: '1',
      trackId: '1',
      position: 1,
    },
    {
      id: '1-2',
      playlistId: '1',
      trackId: '2',
      position: 2,
    },
    {
      id: '1-3',
      playlistId: '1',
      trackId: '3',
      position: 3,
    },
    {
      id: '1-4',
      playlistId: '1',
      trackId: '4',
      position: 4,
    },
  ] as const;

  const tracksArtists = tracks.flatMap(t => {
    const trackId = Number(t.id);
    if (trackId % 2 === 0) {
      // even: all artists
      return artists.map(
        a =>
          ({
            id: `${t.id}-${a.id}`,
            trackId: t.id,
            artistId: a.id,
          }) satisfies TrackArtist,
      );
    }
    // odd: single artist
    return [
      {
        id: `${t.id}-1`,
        trackId: t.id,
        artistId: '1',
      } satisfies TrackArtist,
    ];
  });

  await Promise.all([
    ...tracks.map(z.mutate.track.create),
    ...albums.map(z.mutate.album.create),
    ...artists.map(z.mutate.artist.create),
    ...playlists.map(z.mutate.playlist.create),
    ...tracksArtists.map(z.mutate.trackArtist.create),
    ...playlistTracks.map(z.mutate.playlistTrack.create),
  ]);

  const stmt = z.query.playlistTrack
    .join(z.query.track, 'track', 'trackId', 'id')
    .join(z.query.album, 'album', 'track.albumId', 'id')
    .join(z.query.trackArtist, 'trackArtist', 'track.id', 'trackArtist.trackId')
    .join(z.query.artist, 'artists', 'trackArtist.artistId', 'id')
    .where('playlistTrack.playlistId', '=', '1')
    .groupBy('track.id')
    .select('track.*', agg.array('artists.*', 'artists'))
    .orderBy('track.id', 'asc')
    .prepare();

  const rows = await stmt.exec();

  expect(rows).toEqual([
    {
      id: '1_1-1_1_1_1-1',
      playlistTrack: {id: '1-1', playlistId: '1', trackId: '1', position: 1},
      track: {id: '1', title: 'Track 1', length: 100, albumId: '1'},
      album: {id: '1', title: 'Album 1', artistId: '1'},
      trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
      artists: [{id: '1', name: 'Artist 1'}],
      [joinSymbol]: true,
    },
    {
      id: '1_1_1-2_2_2-1',
      playlistTrack: {id: '1-2', playlistId: '1', trackId: '2', position: 2},
      track: {id: '2', title: 'Track 2', length: 100, albumId: '1'},
      album: {id: '1', title: 'Album 1', artistId: '1'},
      trackArtist: {id: '2-1', trackId: '2', artistId: '1'},
      artists: [
        {id: '1', name: 'Artist 1'},
        {id: '2', name: 'Artist 2'},
        {id: '3', name: 'Artist 3'},
      ],
      [joinSymbol]: true,
    },
    {
      id: '1_1-3_3_2_3-1',
      playlistTrack: {id: '1-3', playlistId: '1', trackId: '3', position: 3},
      track: {id: '3', title: 'Track 3', length: 100, albumId: '2'},
      album: {id: '2', title: 'Album 2', artistId: '1'},
      trackArtist: {id: '3-1', trackId: '3', artistId: '1'},
      artists: [{id: '1', name: 'Artist 1'}],
      [joinSymbol]: true,
    },
    {
      id: '1_1-4_4_2_4-1',
      playlistTrack: {id: '1-4', playlistId: '1', trackId: '4', position: 4},
      track: {id: '4', title: 'Track 4', length: 100, albumId: '2'},
      album: {id: '2', title: 'Album 2', artistId: '1'},
      trackArtist: {id: '4-1', trackId: '4', artistId: '1'},
      artists: [
        {id: '1', name: 'Artist 1'},
        {id: '2', name: 'Artist 2'},
        {id: '3', name: 'Artist 3'},
      ],
      [joinSymbol]: true,
    },
  ]);

  await z.close();
});

test('track list composition with lots and lots of data then tracking incremental changes', async () => {
  const z = newZero();

  const artists = createRandomArtists(100);
  const albums = createRandomAlbums(100, artists);
  const tracks = createRandomTracks(10_000, albums);
  const trackArtists = linkTracksToArtists(artists, tracks);

  await bulkSet(z, {
    tracks,
    albums,
    artists,
    trackArtists,
  });

  const stmt = z.query.track
    .join(z.query.album, 'album', 'track.albumId', 'id')
    .join(z.query.trackArtist, 'trackArtist', 'track.id', 'trackArtist.trackId')
    .join(z.query.artist, 'artists', 'trackArtist.artistId', 'id')
    .groupBy('track.id')
    .select('track.*', agg.array('artists.*', 'artists'))
    .orderBy('track.id', 'asc')
    .prepare();
  let rows = await stmt.exec();
  expect(rows.length).toBe(10_000);

  // add more tracks
  const newTracks = createRandomTracks(100, albums);
  const newTrackArtists = linkTracksToArtists(artists, newTracks);

  await bulkSet(z, {
    tracks: newTracks,
    trackArtists: newTrackArtists,
  });

  // TODO: exec query may have run before we get here. In that
  // the `experimentalWatch` callback has fired and updated the statement.
  rows = await stmt.exec();
  expect(rows.length).toBe(10_100);

  // remove 100 tracks
  const tracksToRemove = newTracks.slice(0, 100);
  await bulkRemove(z, {tracks: tracksToRemove});

  rows = await stmt.exec();
  expect(rows.length).toBe(10_000);
});
