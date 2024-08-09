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

import {QueryDefs} from 'zero-client';
import {QueryParseDefs} from 'zero-client/src/client/options.js';

export const musicAppQueries: QueryParseDefs<QueryDefs> = {
  track: v => v as Track,
  album: v => v as Album,
  artist: v => v as Artist,
  playlist: v => v as Playlist,
  trackArtist: v => v as TrackArtist,
  playlistTrack: v => v as PlaylistTrack,
};
