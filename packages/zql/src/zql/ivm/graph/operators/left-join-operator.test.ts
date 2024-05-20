import {expect, test, vi} from 'vitest';
import {normalize} from '../../multiset.js';
import {JoinResult, joinSymbol} from '../../types.js';
import {DifferenceStream} from '../difference-stream.js';
import {createPullResponseMessage} from '../message.js';

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

test('left join', () => {
  const trackInput = new DifferenceStream<Track>();
  const albumInput = new DifferenceStream<Album>();

  const output = trackInput.leftJoin({
    aAs: 'track',
    getAJoinKey: track => track.albumId,
    getAPrimaryKey: track => track.id,
    b: albumInput,
    bAs: 'album',
    getBJoinKey: album => album.id,
    getBPrimaryKey: album => album.id,
  });

  const items: [JoinResult<Track, Album, 'track', 'album'>, number][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  let version = 1;
  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  ++version;

  // left join will return `tracks` even if they do not match an album
  expect(items).toEqual([
    [
      {
        id: '1',
        [joinSymbol]: true,
        track: {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        // album is missing
      },
      1,
    ],
  ]);
  items.length = 0;

  // now add an album
  albumInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Album One',
          artistId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  albumInput.commit(version);
  ++version;

  // now the album is present
  expect(items).toEqual([
    [
      {
        id: '1_1',
        [joinSymbol]: true,
        track: {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        album: {
          id: '1',
          title: 'Album One',
          artistId: '1',
        },
      },
      1,
    ],
    [
      {
        id: '1',
        [joinSymbol]: true,
        track: {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
      },
      -1,
    ],
  ]);
  items.length = 0;

  // now remove a track.
  // The joined row should be retracted since the track is no longer present.
  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        -1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  ++version;

  // the row is retracted for the track
  expect(items).toEqual([
    [
      {
        id: '1_1',
        [joinSymbol]: true,
        track: {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        album: {
          id: '1',
          title: 'Album One',
          artistId: '1',
        },
      },
      -1,
    ],
  ]);
  items.length = 0;

  // now remove the album. Nothing should be output since there
  // was nothing output last time.
  albumInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Album One',
          artistId: '1',
        },
        -1,
      ],
    ],
    undefined,
  );
  albumInput.commit(version);
  ++version;

  expect(items).toEqual([]);
  items.length = 0;

  // now add an album first
  // nothing should be output since there's no track on the left to join with the album
  albumInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Album One',
          artistId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  albumInput.commit(version);
  ++version;

  expect(items).toEqual([]);
  items.length = 0;

  // now add a track back
  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  ++version;

  // now the join is present
  expect(items).toEqual([
    [
      {
        id: '1_1',
        [joinSymbol]: true,
        track: {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        album: {
          id: '1',
          title: 'Album One',
          artistId: '1',
        },
      },
      1,
    ],
  ]);
});

test('junction table left join', () => {
  let version = 0;
  const trackInput = new DifferenceStream<Track>();
  const trackArtistInput = new DifferenceStream<TrackArtist>();
  const artistInput = new DifferenceStream<Artist>();

  const trackTrackArtist = trackInput.leftJoin({
    aAs: 'track',
    getAJoinKey: track => track.id,
    getAPrimaryKey: track => track.id,
    b: trackArtistInput,
    bAs: 'trackArtist',
    getBJoinKey: trackArtist => trackArtist.trackId,
    getBPrimaryKey: trackArtist => trackArtist.id,
  });

  const output = trackTrackArtist.leftJoin({
    aAs: undefined,
    getAJoinKey: x => x.trackArtist?.artistId,
    getAPrimaryKey: x => x?.id,
    b: artistInput,
    bAs: 'artist',
    getBJoinKey: artist => artist.id,
    getBPrimaryKey: artist => artist.id,
  });

  const items: [
    JoinResult<Track, TrackArtist, 'track', 'trackArtist'>,
    number,
  ][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  trackArtistInput.newDifference(
    version,
    [
      [
        {
          id: '1-1',
          trackId: '1',
          artistId: '1',
        },
        1,
      ],
      [
        {
          id: '1-2',
          trackId: '1',
          artistId: '2',
        },
        1,
      ],
    ],
    undefined,
  );
  artistInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          name: 'Artist One',
        },
        1,
      ],
      [
        {
          id: '2',
          name: 'Artist Two',
        },
        1,
      ],
    ],
    undefined,
  );

  trackInput.commit(version);
  trackArtistInput.commit(version);
  artistInput.commit(version);

  // Because we do not queue values at our inputs
  // left-join sends a bunch of extra rows
  // as it eagerly executes each time a value arrives at any input.
  // If you were to normalize the below it'll be the desired result.
  expect(items).toEqual([
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        artist: {id: '1', name: 'Artist One'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        artist: {id: '2', name: 'Artist Two'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1-2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        [joinSymbol]: true,
      },
      -1,
    ],
  ]);

  // check that the above junk, when normalized, is what the join should be.
  expect([...normalize(items, x => x.id)]).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        artist: {id: '1', name: 'Artist One'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        artist: {id: '2', name: 'Artist Two'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);

  items.length = 0;
  ++version;

  // remove the track
  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        -1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  ++version;

  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        artist: {id: '1', name: 'Artist One'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        artist: {id: '2', name: 'Artist Two'},
        [joinSymbol]: true,
      },
      -1,
    ],
  ]);
  items.length = 0;

  // re-add the track
  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  ++version;

  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        artist: {id: '1', name: 'Artist One'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        artist: {id: '2', name: 'Artist Two'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
  items.length = 0;

  // remove the track-artist links
  trackArtistInput.newDifference(
    version,
    [
      [
        {
          id: '1-1',
          trackId: '1',
          artistId: '1',
        },
        -1,
      ],
      [
        {
          id: '1-2',
          trackId: '1',
          artistId: '2',
        },
        -1,
      ],
    ],
    undefined,
  );
  trackArtistInput.commit(version);
  ++version;
  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        artist: {id: '1', name: 'Artist One'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        artist: {id: '2', name: 'Artist Two'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1',
        track: {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
  items.length = 0;

  // add the track-artist link
  trackArtistInput.newDifference(
    version,
    [
      [
        {
          id: '1-1',
          trackId: '1',
          artistId: '1',
        },
        1,
      ],
      [
        {
          id: '1-2',
          trackId: '1',
          artistId: '2',
        },
        1,
      ],
    ],
    undefined,
  );
  trackArtistInput.commit(version);
  ++version;

  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        artist: {id: '1', name: 'Artist One'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1',
        track: {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        artist: {id: '2', name: 'Artist Two'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
  items.length = 0;

  // remove the artist
  artistInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          name: 'Artist One',
        },
        -1,
      ],
      [
        {
          id: '2',
          name: 'Artist Two',
        },
        -1,
      ],
    ],
    undefined,
  );
  artistInput.commit(version);
  ++version;
  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        artist: {id: '1', name: 'Artist One'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        artist: {id: '2', name: 'Artist Two'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
  items.length = 0;

  // add a track with no links to anyone
  trackInput.newDifference(
    version,
    [
      [
        {
          id: '2',
          title: 'Track Two',
          length: 1,
          albumId: '2',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  ++version;

  expect(items).toEqual([
    [
      {
        id: '2',
        track: {id: '2', title: 'Track Two', length: 1, albumId: '2'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
});

test('repro 1', () => {
  let version = 0;
  const trackInput = new DifferenceStream<Track>();
  const trackArtistInput = new DifferenceStream<TrackArtist>();
  const artistInput = new DifferenceStream<Artist>();

  const trackTrackArtist = trackInput.leftJoin({
    aAs: 'track',
    getAJoinKey: track => track.id,
    getAPrimaryKey: track => track.id,
    b: trackArtistInput,
    bAs: 'trackArtist',
    getBJoinKey: trackArtist => trackArtist.trackId,
    getBPrimaryKey: trackArtist => trackArtist.id,
  });

  const output = trackTrackArtist.leftJoin({
    aAs: undefined,
    getAJoinKey: x => x.trackArtist?.artistId,
    getAPrimaryKey: x => x?.id,
    b: artistInput,
    bAs: 'artist',
    getBJoinKey: artist => artist.id,
    getBPrimaryKey: artist => artist.id,
  });

  const items: [
    JoinResult<Track, TrackArtist, 'track', 'trackArtist'>,
    number,
  ][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  ++version;

  artistInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          name: 'Artist One',
        },
        1,
      ],
      [
        {
          id: '2',
          name: 'Artist Two',
        },
        1,
      ],
    ],
    undefined,
  );
  artistInput.commit(version);
  ++version;

  items.length = 0;
  trackArtistInput.newDifference(
    version,
    [
      [
        {
          id: '1-1',
          trackId: '1',
          artistId: '1',
        },
        1,
      ],
      [
        {
          id: '1-2',
          trackId: '1',
          artistId: '2',
        },
        1,
      ],
    ],
    undefined,
  );
  trackArtistInput.commit(version);
  ++version;
  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        artist: {id: '1', name: 'Artist One'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        artist: {id: '2', name: 'Artist Two'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
  items.length = 0;

  trackArtistInput.newDifference(
    version,
    [
      [
        {
          id: '1-1',
          trackId: '1',
          artistId: '1',
        },
        -1,
      ],
      [
        {
          id: '1-2',
          trackId: '1',
          artistId: '2',
        },
        -1,
      ],
    ],
    undefined,
  );
  trackArtistInput.commit(version);
  ++version;
  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        artist: {id: '1', name: 'Artist One'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        artist: {id: '2', name: 'Artist Two'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
});

test('add track & album, then remove album', () => {
  const trackInput = new DifferenceStream<Track>();
  const albumInput = new DifferenceStream<Album>();

  const output = trackInput.leftJoin({
    aAs: 'track',
    getAJoinKey: track => track.albumId,
    getAPrimaryKey: track => track.id,
    b: albumInput,
    bAs: 'album',
    getBJoinKey: album => album.id,
    getBPrimaryKey: album => album.id,
  });

  const items: [JoinResult<Track, Album, 'track', 'album'>, number][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  let version = 1;
  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  albumInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Album One',
          artistId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  albumInput.commit(version);
  ++version;

  expect(items).toEqual([
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        album: {id: '1', title: 'Album One', artistId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      -1,
    ],
  ]);
  items.length = 0;

  // retract the album
  albumInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Album One',
          artistId: '1',
        },
        -1,
      ],
    ],
    undefined,
  );
  albumInput.commit(version);
  ++version;

  expect(items).toEqual([
    [
      {
        id: '1_1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        album: {id: '1', title: 'Album One', artistId: '1'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
});

test('one to many, remove the one, add the one', () => {
  let version = 0;
  const trackInput = new DifferenceStream<Track>();
  const trackArtistInput = new DifferenceStream<TrackArtist>();

  const output = trackInput.leftJoin({
    aAs: 'track',
    getAJoinKey: track => track.id,
    getAPrimaryKey: track => track.id,
    b: trackArtistInput,
    bAs: 'trackArtist',
    getBJoinKey: trackArtist => trackArtist.trackId,
    getBPrimaryKey: trackArtist => trackArtist.id,
  });

  const items: [
    JoinResult<Track, TrackArtist, 'track', 'trackArtist'>,
    number,
  ][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  trackArtistInput.newDifference(
    version,
    [
      [
        {
          id: '1-1',
          trackId: '1',
          artistId: '1',
        },
        1,
      ],
      [
        {
          id: '1-2',
          trackId: '1',
          artistId: '2',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  trackArtistInput.commit(version);
  ++version;

  expect(items).toEqual([
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
  items.length = 0;

  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        -1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  ++version;

  expect(items).toEqual([
    [
      {
        id: '1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        [joinSymbol]: true,
      },
      -1,
    ],
  ]);
  items.length = 0;

  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  ++version;

  expect(items).toEqual([
    [
      {
        id: '1_1-1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1-2',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
  items.length = 0;
});

test('two tracks, only 1 is linked to artists', () => {
  let version = 0;
  const trackInput = new DifferenceStream<Track>();
  const trackArtistInput = new DifferenceStream<TrackArtist>();
  const artistInput = new DifferenceStream<Artist>();

  const trackTrackArtist = trackInput.leftJoin({
    aAs: 'track',
    getAJoinKey: track => track.id,
    getAPrimaryKey: track => track.id,
    b: trackArtistInput,
    bAs: 'trackArtist',
    getBJoinKey: trackArtist => trackArtist.trackId,
    getBPrimaryKey: trackArtist => trackArtist.id,
  });

  const output = trackTrackArtist.leftJoin({
    aAs: undefined,
    getAJoinKey: x => x.trackArtist?.artistId,
    getAPrimaryKey: x => x?.id,
    b: artistInput,
    bAs: 'artist',
    getBJoinKey: artist => artist.id,
    getBPrimaryKey: artist => artist.id,
  });

  const items: [
    JoinResult<Track, TrackArtist, 'track', 'trackArtist'>,
    number,
  ][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  trackInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          title: 'Track One',
          length: 1,
          albumId: '1',
        },
        1,
      ],
      [
        {
          id: '2',
          albumId: '1',
          title: 'track 2',
          length: 1,
        },
        1,
      ],
    ],
    undefined,
  );
  artistInput.newDifference(
    version,
    [
      [
        {
          id: '1',
          name: 'Artist One',
        },
        1,
      ],
      [
        {
          id: '2',
          name: 'Artist Two',
        },
        1,
      ],
    ],
    undefined,
  );
  trackArtistInput.newDifference(
    version,
    [
      [
        {
          id: '1-1',
          trackId: '1',
          artistId: '1',
        },
        1,
      ],
      [
        {
          id: '1-2',
          trackId: '1',
          artistId: '2',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);
  artistInput.commit(version);
  trackArtistInput.commit(version);

  ++version;
  expect(items).toEqual([
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '2',
        track: {id: '2', albumId: '1', title: 'track 2', length: 1},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1_1-1',
        trackArtist: {id: '1-1', trackId: '1', artistId: '1'},
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        artist: {id: '1', name: 'Artist One'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1',
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        [joinSymbol]: true,
      },
      -1,
    ],
    [
      {
        id: '1_1-2_2',
        trackArtist: {id: '1-2', trackId: '1', artistId: '2'},
        track: {id: '1', title: 'Track One', length: 1, albumId: '1'},
        artist: {id: '2', name: 'Artist Two'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
});

test('order is removed from request', () => {
  orderIsRemovedFromRequest('leftJoin');
});

test('order is removed from reply', () => {
  orderIsRemovedFromReply('leftJoin');
});

export function orderIsRemovedFromRequest(join: 'leftJoin' | 'join') {
  const trackInput = new DifferenceStream<Track>();
  const albumInput = new DifferenceStream<Album>();
  const output = trackInput[join]({
    aAs: 'track',
    getAJoinKey: x => x.albumId,
    getAPrimaryKey: x => x.id,
    b: albumInput,
    bAs: 'album',
    getBJoinKey: x => x.id,
    getBPrimaryKey: x => x.id,
  });

  const trackInputSpy = vi.spyOn(trackInput, 'messageUpstream');
  const albumInputSpy = vi.spyOn(albumInput, 'messageUpstream');

  const msg = {
    id: 1,
    hoistedConditions: [],
    type: 'pull',
    order: [[['intentional-nonsense', 'x']], 'asc'],
  } as const;
  const listener = {
    commit() {},
    newDifference() {},
  };
  output.messageUpstream(msg, listener);

  expect(trackInputSpy).toHaveBeenCalledOnce();
  expect(albumInputSpy).toHaveBeenCalledOnce();

  expect(trackInputSpy.mock.calls[0][0]).toEqual(msg);
  expect(albumInputSpy.mock.calls[0][0]).toEqual({...msg, order: undefined});
}

export function orderIsRemovedFromReply(join: 'leftJoin' | 'join') {
  const trackInput = new DifferenceStream<Track>();
  const albumInput = new DifferenceStream<Album>();
  const output = trackInput[join]({
    aAs: 'track',
    getAJoinKey: x => x.albumId,
    getAPrimaryKey: x => x.id,
    b: albumInput,
    bAs: 'album',
    getBJoinKey: x => x.id,
    getBPrimaryKey: x => x.id,
  });

  const outputSpy = vi.spyOn(output, 'newDifference');
  const msg = {
    id: 1,
    hoistedConditions: [],
    type: 'pull',
    order: [[['intentional-nonsense', 'x']], 'asc'],
  } as const;
  const listener = {
    commit() {},
    newDifference() {},
  };
  output.messageUpstream(msg, listener);
  const trackReply = createPullResponseMessage(msg, 'track', [
    [['track', 'id']],
    'asc',
  ]);
  const albumReply = createPullResponseMessage(msg, 'title', [
    [['title', 'id']],
    'asc',
  ]);

  trackInput.newDifference(1, [], trackReply);

  // join buffers until both replies are received.
  expect(outputSpy).toHaveBeenCalledTimes(0);

  albumInput.newDifference(1, [], albumReply);

  expect(outputSpy).toHaveBeenCalledTimes(1);
  expect(outputSpy.mock.calls[0][0]).toEqual(1);
  expect([...outputSpy.mock.calls[0][1]]).toEqual([]);
  expect(outputSpy.mock.calls[0][2]).toEqual(trackReply);
}
