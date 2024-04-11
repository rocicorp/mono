/*
Test join in all its glory.

Test cases:
- Test join with a single table
- Test join with multiple tables
- Test join with a where clause
- Test join with a where clause and multiple tables
- Test ordering the join
- Test join with limit
- Test join with limit and ordering
- Test join with limit and ordering and where clause
- Test join with limit and ordering and where clause and multiple tables
- Test join with limit and ordering and where clause and multiple tables and multiple where clauses
- Test join with limit and ordering and where clause and multiple tables and multiple where clauses and multiple order by clauses
- Test join with limit and ordering and where clause and multiple tables and multiple where clauses and multiple order by clauses and multiple group by clauses

- large scale joins
- deltas flowing through the system
- time the delta computations

- Test `after` with join

Should sort the join and make it a source?
*/

// test('join with a single table')

type Track = {
  id: string;
  title: string;
  length: number;
  albumId: number;
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
  trackId: number;
  artistId: number;
};

type PlaylistTrack = {
  id: `${PlaylistTrack['playlistId']}-${PlaylistTrack['trackId']}`;
  playlistId: number;
  trackId: number;
  position: number;
};

test('tracks and their albums', () => {});
