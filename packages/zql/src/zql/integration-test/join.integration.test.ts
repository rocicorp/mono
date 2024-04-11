import {generate} from '@rocicorp/rails';
import {nanoid} from 'nanoid';
import {Replicache, TEST_LICENSE_KEY} from 'replicache';
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
    .select('track.*', agg.array('artists.name', 'artists'))
    .asc('track.id')
    .prepare();

  const rows = await stmt.exec();
  console.log(rows);

  await r.close();
});

// Observations / future things to test:
// - joining against a collection that had no writes hung forever.
// -  it'd be nice if we could aggregate the whole row into an array, not just a column of the row
//    `AggArray` would need to be able to take a table name as a selector.
//    an example of this is `agg.array('artists.name', 'artists')`
//    we really want to full `artists` row, not just the name.
// - agg array not following a qualified selector
