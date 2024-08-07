import type {
  Album,
  Artist,
  Playlist,
  PlaylistTrack,
  Track,
  TrackArtist,
} from 'zql/src/zql/test-helpers/create-data.js';
export {
  createRandomAlbums,
  createRandomArtists,
  createRandomTracks,
  linkTracksToArtists,
} from 'zql/src/zql/test-helpers/create-data.js';
export {Album, Artist, Playlist, PlaylistTrack, Track, TrackArtist};
import {ZqlLiteZero} from './zqlite-zero.js';
import Database from 'better-sqlite3';

export function newZero(dbInit?: Database.Database | undefined) {
  const db = dbInit ? dbInit : new Database(':memory:');
  const z = new ZqlLiteZero({
    queries: {
      track: v => v as Track,
      album: v => v as Album,
      artist: v => v as Artist,
      playlist: v => v as Playlist,
      trackArtist: v => v as TrackArtist,
      playlistTrack: v => v as PlaylistTrack,
    },
    db,
  });
  if (!db) {
    throw new Error('db is undefined');
  }
  db.prepare('CREATE TABLE artist (id TEXT PRIMARY KEY, name TEXT)').run();
  db.prepare(
    'CREATE TABLE album (id TEXT PRIMARY KEY, title TEXT, artistId TEXT)',
  ).run();
  db.prepare(
    'CREATE TABLE track (id TEXT PRIMARY KEY, length NUMBER, title TEXT, albumId TEXT)',
  ).run();
  db.prepare(
    'CREATE TABLE trackArtist (id TEXT PRIMARY KEY, artistId TEXT, trackId TEXT)',
  ).run();
  return z;
}

export type Z = ReturnType<typeof newZero>;

export async function bulkSet(
  z: Z,
  items: {
    tracks?: readonly Track[] | undefined;
    albums?: readonly Album[] | undefined;
    artists?: readonly Artist[] | undefined;
    playlists?: readonly Playlist[] | undefined;
    trackArtists?: readonly TrackArtist[] | undefined;
  },
) {
  const promises: Promise<void>[] = [];
  await z.mutate(async tx => {
    for (const track of items.tracks ?? []) {
      promises.push(tx.track.create(track));
    }
    for (const album of items.albums ?? []) {
      promises.push(tx.album.create(album));
    }
    for (const artist of items.artists ?? []) {
      promises.push(tx.artist.create(artist));
    }
    for (const playlist of items.playlists ?? []) {
      promises.push(tx.playlist.create(playlist));
    }
    for (const trackArtist of items.trackArtists ?? []) {
      promises.push(tx.trackArtist.create(trackArtist));
    }
    await Promise.all(promises);
  });
}

export async function bulkRemove(
  z: Z,
  items: {
    tracks?: Track[] | undefined;
    albums?: Album[] | undefined;
    artists?: Artist[] | undefined;
    playlists?: Playlist[] | undefined;
    trackArtists?: TrackArtist[] | undefined;
  },
) {
  await z.mutate(async tx => {
    const promises: Promise<void>[] = [];
    for (const track of items.tracks ?? []) {
      promises.push(tx.track.delete({id: track.id}));
    }
    for (const album of items.albums ?? []) {
      promises.push(tx.album.delete(album));
    }
    for (const artist of items.artists ?? []) {
      promises.push(tx.artist.delete(artist));
    }
    for (const playlist of items.playlists ?? []) {
      promises.push(tx.playlist.delete(playlist));
    }
    for (const trackArtist of items.trackArtists ?? []) {
      promises.push(tx.trackArtist.delete(trackArtist));
    }
    await Promise.all(promises);
  });
}
