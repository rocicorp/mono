import {generate} from '@rocicorp/rails';
import {nanoid} from 'nanoid';
import {Replicache, TEST_LICENSE_KEY, WriteTransaction} from 'replicache';
import {makeReplicacheContext} from '../context/replicache-context.js';
import {EntityQuery} from '../query/entity-query.js';

export type Track = {
  id: string;
  title: string;
  length: number;
  albumId: string;
};

export type Album = {
  id: string;
  title: string;
  artistId: string;
};

export type Artist = {
  id: string;
  name: string;
};

export type Playlist = {
  id: string;
  name: string;
};

export type TrackArtist = {
  id: `${TrackArtist['trackId']}-${TrackArtist['artistId']}`;
  trackId: string;
  artistId: string;
};

export type PlaylistTrack = {
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

export function setup() {
  const r = newRep();
  const c = makeReplicacheContext(r, {
    subscriptionAdded(_ast) {},
    subscriptionRemoved(_ast) {},
  });
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

export function createRandomArtists(
  n: number,
  autoIncr: boolean = false,
): Artist[] {
  let id = 0;
  return Array.from({length: n}, () => ({
    id: autoIncr ? `${++id}` : nanoid(),
    name: autoIncr ? `Artist ${id}` : nanoid(),
  }));
}

export function createRandomAlbums(
  n: number,
  artists: Artist[],
  autoIncr: boolean = false,
): Album[] {
  let id = 0;
  return Array.from({length: n}, () => ({
    id: autoIncr ? `${++id}` : nanoid(),
    title: autoIncr ? `Album ${id}` : nanoid(),
    artistId: autoIncr
      ? artists[0].id
      : artists[Math.floor(Math.random() * artists.length)].id,
  }));
}

export function createRandomTracks(
  n: number,
  albums: Album[],
  autoIncr: boolean = false,
): Track[] {
  let id = 0;
  return Array.from({length: n}, () => ({
    id: autoIncr ? `${++id}` : nanoid(),
    title: autoIncr ? `Track ${id}` : nanoid(),
    length: autoIncr ? id * 1000 : Math.floor(Math.random() * 300000) + 1000,
    albumId: autoIncr
      ? albums[0].id
      : albums[Math.floor(Math.random() * albums.length)].id,
  }));
}

export function linkTracksToArtists(
  artists: Artist[],
  tracks: Track[],
  assignAll: boolean = false,
): TrackArtist[] {
  // assign each track to 1-3 artists
  return tracks.flatMap(t => {
    const numArtists = assignAll
      ? artists.length
      : Math.floor(Math.random() * 3) + 1;
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
