import type {
  Album,
  Artist,
  Playlist,
  PlaylistTrack,
  Track,
  TrackArtist,
} from './benchmarks/setup.js';
import {nanoid} from '../../util/nanoid.js';
import {Zero} from '../zero.js';
export {
  createRandomAlbums,
  createRandomArtists,
  createRandomTracks,
  linkTracksToArtists,
} from './benchmarks/setup.js';
export {Album, Artist, Playlist, PlaylistTrack, Track, TrackArtist};

export function newZero() {
  const z = new Zero({
    userID: 'user-' + nanoid(),
    queries: {
      track: v => v as Track,
      album: v => v as Album,
      artist: v => v as Artist,
      playlist: v => v as Playlist,
      trackArtist: v => v as TrackArtist,
      playlistTrack: v => v as PlaylistTrack,
    },
  });
  return z;
}

export type Z = ReturnType<typeof newZero>;

export async function bulkSet(
  z: Z,
  items: {
    tracks?: Track[] | undefined;
    albums?: Album[] | undefined;
    artists?: Artist[] | undefined;
    playlists?: Playlist[] | undefined;
    trackArtists?: TrackArtist[] | undefined;
  },
) {
  const promises: Promise<void>[] = [];
  for (const track of items.tracks ?? []) {
    promises.push(z.mutate.track.create(track));
  }
  for (const album of items.albums ?? []) {
    promises.push(z.mutate.album.create(album));
  }
  for (const artist of items.artists ?? []) {
    promises.push(z.mutate.artist.create(artist));
  }
  for (const playlist of items.playlists ?? []) {
    promises.push(z.mutate.playlist.create(playlist));
  }
  for (const trackArtist of items.trackArtists ?? []) {
    promises.push(z.mutate.trackArtist.create(trackArtist));
  }
  await Promise.all(promises);
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
  const promises: Promise<void>[] = [];
  for (const track of items.tracks ?? []) {
    promises.push(z.mutate.track.delete({id: track.id}));
  }
  for (const album of items.albums ?? []) {
    promises.push(z.mutate.album.delete({id: album.id}));
  }
  for (const artist of items.artists ?? []) {
    promises.push(z.mutate.artist.delete({id: artist.id}));
  }
  for (const playlist of items.playlists ?? []) {
    promises.push(z.mutate.playlist.delete({id: playlist.id}));
  }
  for (const trackArtist of items.trackArtists ?? []) {
    promises.push(z.mutate.trackArtist.delete({id: trackArtist.id}));
  }
  await Promise.all(promises);
}
