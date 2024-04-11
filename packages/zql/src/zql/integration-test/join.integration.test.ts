import {generate} from '@rocicorp/rails';
import {nanoid} from 'nanoid';
import {Replicache, TEST_LICENSE_KEY, WriteTransaction} from 'replicache';
import {expect, test} from 'vitest';
import {makeReplicacheContext} from '../context/replicache-context.js';
import {EntityQuery} from '../query/entity-query.js';
import {joinSymbol} from '../ivm/types.js';
import * as agg from '../query/agg.js';

// test('join with a single table')

type Track = {
  id: string;
  title: string;
  length: number;
  albumId: string;
};

type Album = {
  id: string;
  title: string;
  artistId: string;
};

type Artist = {
  id: string;
  name: string;
};

type Playlist = {
  id: string;
  name: string;
};

type TrackArtist = {
  id: `${TrackArtist['trackId']}-${TrackArtist['artistId']}`;
  trackId: string;
  artistId: string;
};

type PlaylistTrack = {
  id: `${PlaylistTrack['playlistId']}-${PlaylistTrack['trackId']}`;
  playlistId: string;
  trackId: string;
  position: number;
};

const {
  init: initTrack,
  set: setTrack,
  update: updateTrack,
  delete: deleteTrack,
} = generate<Track>('track');

const {
  init: initAlbum,
  set: setAlbum,
  update: updateAlbum,
  delete: deleteAlbum,
} = generate<Album>('album');

const {
  init: initArtist,
  set: setArtist,
  update: updateArtist,
  delete: deleteArtist,
} = generate<Artist>('artist');

const {
  init: initPlaylist,
  set: setPlaylist,
  update: updatePlaylist,
  delete: deletePlaylist,
} = generate<Playlist>('playlist');

const {
  init: initTrackArtist,
  set: setTrackArtist,
  update: updateTrackArtist,
  delete: deleteTrackArtist,
} = generate<TrackArtist>('trackArtist');

const {
  init: initPlaylistTrack,
  set: setPlaylistTrack,
  update: updatePlaylistTrack,
  delete: deletePlaylistTrack,
} = generate<PlaylistTrack>('playlistTrack');

const mutators = {
  initTrack,
  setTrack,
  updateTrack,
  deleteTrack,
  initAlbum,
  setAlbum,
  updateAlbum,
  deleteAlbum,
  initArtist,
  setArtist,
  updateArtist,
  deleteArtist,
  initPlaylist,
  setPlaylist,
  updatePlaylist,
  deletePlaylist,
  initTrackArtist,
  setTrackArtist,
  updateTrackArtist,
  deleteTrackArtist,
  initPlaylistTrack,
  setPlaylistTrack,
  updatePlaylistTrack,
  deletePlaylistTrack,
  bulkSet: async (
    tx: WriteTransaction,
    items: {
      tracks?: Track[] | undefined;
      albums?: Album[] | undefined;
      artists?: Artist[] | undefined;
      playlists?: Playlist[] | undefined;
      trackArtists?: TrackArtist[] | undefined;
    },
  ) => {
    const promises: Promise<void>[] = [];
    for (const track of items.tracks ?? []) {
      promises.push(tx.set(`track/${track.id}`, track));
    }
    for (const album of items.albums ?? []) {
      promises.push(tx.set(`album/${album.id}`, album));
    }
    for (const artist of items.artists ?? []) {
      promises.push(tx.set(`artist/${artist.id}`, artist));
    }
    for (const playlist of items.playlists ?? []) {
      promises.push(tx.set(`playlist/${playlist.id}`, playlist));
    }
    for (const trackArtist of items.trackArtists ?? []) {
      promises.push(tx.set(`trackArtist/${trackArtist.id}`, trackArtist));
    }

    await Promise.all(promises);
  },
  bulkRemove: async (
    tx: WriteTransaction,
    items: {
      tracks?: Track[] | undefined;
      albums?: Album[] | undefined;
      artists?: Artist[] | undefined;
      playlists?: Playlist[] | undefined;
      trackArtists?: TrackArtist[] | undefined;
    },
  ) => {
    const promises: Promise<boolean>[] = [];
    for (const track of items.tracks ?? []) {
      promises.push(tx.del(`track/${track.id}`));
    }
    for (const album of items.albums ?? []) {
      promises.push(tx.del(`album/${album.id}`));
    }
    for (const artist of items.artists ?? []) {
      promises.push(tx.del(`artist/${artist.id}`));
    }
    for (const playlist of items.playlists ?? []) {
      promises.push(tx.del(`playlist/${playlist.id}`));
    }
    for (const trackArtist of items.trackArtists ?? []) {
      promises.push(tx.del(`trackArtist/${trackArtist.id}`));
    }

    await Promise.all(promises);
  },
};

function newRep() {
  return new Replicache({
    licenseKey: TEST_LICENSE_KEY,
    name: nanoid(),
    mutators,
  });
}

function setup() {
  const r = newRep();
  const c = makeReplicacheContext(r);
  const trackQuery = new EntityQuery<{track: Track}>(c, 'track');
  const albumQuery = new EntityQuery<{album: Album}>(c, 'album');
  const artistQuery = new EntityQuery<{artist: Artist}>(c, 'artist');
  const playlistQuery = new EntityQuery<{playlist: Playlist}>(c, 'playlist');
  const trackArtistQuery = new EntityQuery<{trackArtist: TrackArtist}>(
    c,
    'trackArtist',
  );
  const playlistTrackQuery = new EntityQuery<{playlistTrack: PlaylistTrack}>(
    c,
    'playlistTrack',
  );

  return {
    r,
    c,
    trackQuery,
    albumQuery,
    artistQuery,
    playlistQuery,
    trackArtistQuery,
    playlistTrackQuery,
  };
}

test('direct foreign key join: join a track to an album', async () => {
  const {r, trackQuery, albumQuery} = setup();

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

  await Promise.all([r.mutate.initTrack(track), r.mutate.initAlbum(album)]);

  const stmt = await trackQuery
    .join(albumQuery, 'album', 'albumId', 'id')
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
  await r.mutate.deleteTrack(track.id);

  rows = await stmt.exec();
  expect(rows).toEqual([]);

  // re-add a track for that album
  await r.mutate.initTrack({
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
  await r.mutate.initAlbum({
    id: '2',
    title: 'Album 2',
    artistId: '1',
  });

  rows = await stmt.exec();
  expect(rows).toEqual([track2Album1]);

  // add an unrelated track
  await r.mutate.initTrack({
    id: '3',
    title: 'Track 3',
    length: 100,
    albumId: '3',
  });

  rows = await stmt.exec();
  expect(rows).toEqual([track2Album1]);

  // add an album related to track3
  await r.mutate.initAlbum({
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
  await r.mutate.initTrack({
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
  expect(rows).toEqual([track2Album1, track4Album2, track3Album3]);

  // add a second track to album 1
  await r.mutate.initTrack({
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
    track5Album1,
    track4Album2,
    track3Album3,
  ]);

  // sort by track id
  const stmt2 = await trackQuery
    .join(albumQuery, 'album', 'albumId', 'id')
    .select('*')
    .asc('track.id')
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
    r.mutate.deleteTrack('2'),
    r.mutate.deleteTrack('3'),
    r.mutate.deleteTrack('4'),
    r.mutate.deleteAlbum('1'),
    r.mutate.deleteAlbum('2'),
    r.mutate.deleteAlbum('3'),
  ]);

  rows = await stmt.exec();
  expect(rows).toEqual([]);

  await r.close();
});

test('junction table join: join a track to its artists', async () => {});

/**
 * A playlist has tracks.
 * Tracks should join in their albums and artists.
 * Artists should be aggregated into an array of artists for a given track, resulting in a single
 * row per track.
 */
test('junction and foreign key join, followed by aggregation: compose a playlist via a join and group by', async () => {
  const {
    r,
    trackQuery,
    albumQuery,
    artistQuery,
    trackArtistQuery,
    playlistTrackQuery,
  } = setup();

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
    ...tracks.map(r.mutate.initTrack),
    ...albums.map(r.mutate.initAlbum),
    ...artists.map(r.mutate.initArtist),
    ...playlists.map(r.mutate.initPlaylist),
    ...tracksArtists.map(r.mutate.initTrackArtist),
    ...playlistTracks.map(r.mutate.initPlaylistTrack),
  ]);

  const stmt = playlistTrackQuery
    .join(trackQuery, 'track', 'trackId', 'id')
    .join(albumQuery, 'album', 'track.albumId', 'id')
    .join(trackArtistQuery, 'trackArtist', 'track.id', 'trackArtist.trackId')
    .join(artistQuery, 'artists', 'trackArtist.artistId', 'id')
    .where('playlistTrack.playlistId', '=', '1')
    .groupBy('track.id')
    .select('track.*', agg.array('artists.*', 'artists'))
    .asc('track.id')
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

  await r.close();
});

test('track list composition with lots and lots of data then tracking incremental changes', async () => {
  const {r, trackQuery, albumQuery, artistQuery, trackArtistQuery} = setup();

  const artists = createRandomArtists(100);
  const albums = createRandomAlbums(100, artists);
  const tracks = createRandomTracks(10_000, albums);
  const trackArtists = linkTracksToArtists(artists, tracks);

  await r.mutate.bulkSet({
    tracks,
    albums,
    artists,
    trackArtists,
    playlists: [],
  });

  const stmt = trackQuery
    .join(albumQuery, 'album', 'track.albumId', 'id')
    .join(trackArtistQuery, 'trackArtist', 'track.id', 'trackArtist.trackId')
    .join(artistQuery, 'artists', 'trackArtist.artistId', 'id')
    .groupBy('track.id')
    .select('track.*', agg.array('artists.*', 'artists'))
    .asc('track.id')
    .prepare();

  let rows = await stmt.exec();
  expect(rows.length).toBe(10_000);

  // add 100 more tracks
  const newArtists = createRandomArtists(100);
  const newAlbums = createRandomAlbums(100, newArtists);
  const newTracks = createRandomTracks(100, newAlbums);
  const newTrackArtists = linkTracksToArtists(newArtists, newTracks);

  await r.mutate.bulkSet({
    tracks: newTracks,
    albums: newAlbums,
    artists: newArtists,
    trackArtists: newTrackArtists,
    playlists: [],
  });

  // TODO: exec query may have run before we get here. In that
  // the `experimentalWatch` callback has fired and updated the statement.
  rows = await stmt.exec();
  expect(rows.length).toBe(10_100);

  // remove 100 tracks
  const tracksToRemove = newTracks.slice(0, 100);
  await r.mutate.bulkRemove({tracks: tracksToRemove});

  rows = await stmt.exec();
  expect(rows.length).toBe(10_000);
});

function createRandomArtists(n: number): Artist[] {
  return Array.from({length: n}, () => ({
    id: nanoid(),
    name: nanoid(),
  }));
}

function createRandomAlbums(n: number, artists: Artist[]): Album[] {
  return Array.from({length: n}, () => ({
    id: nanoid(),
    title: nanoid(),
    artistId: artists[Math.floor(Math.random() * artists.length)].id,
  }));
}

function createRandomTracks(n: number, albums: Album[]): Track[] {
  return Array.from({length: n}, () => ({
    id: nanoid(),
    title: nanoid(),
    length: Math.floor(Math.random() * 300000) + 1000,
    albumId: albums[Math.floor(Math.random() * albums.length)].id,
  }));
}

function linkTracksToArtists(
  artists: Artist[],
  tracks: Track[],
): TrackArtist[] {
  // assign each track to 1-3 artists
  return tracks.flatMap(t => {
    const numArtists = Math.floor(Math.random() * 3) + 1;
    const artistsForTrack = new Set<string>();
    while (artistsForTrack.size < numArtists) {
      artistsForTrack.add(
        artists[Math.floor(Math.random() * artists.length)].id,
      );
    }
    return [...artistsForTrack].map(a => ({
      id: `${t.id}-${a}`,
      trackId: t.id,
      artistId: a,
    }));
  });
}

// Observations / future things to test:
// - we should add `HAVING` to the language. It'll let us filter against compeleted aggregations.
