import {
  mutators,
  type Album,
  type Artist,
  type Playlist,
  type PlaylistTrack,
  type Track,
  type TrackArtist,
} from './benchmarks/setup.js';
import {nanoid} from '../../util/nanoid.js';
import {Zero} from '../zero.js';
export {
  createRandomAlbums,
  createRandomArtists,
  createRandomTracks,
  linkTracksToArtists,
} from './benchmarks/setup.js';
export type {Album, Artist, Playlist, PlaylistTrack, Track, TrackArtist};

export function newZero() {
  const z = new Zero({
    userID: 'user-' + nanoid(),
    roomID: 'room-' + nanoid(),
    mutators,
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
