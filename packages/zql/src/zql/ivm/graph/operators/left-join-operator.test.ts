import {expect, test} from 'vitest';
import {normalize} from '../../multiset.js';
import {JoinResult, joinSymbol} from '../../types.js';
import {DifferenceStream} from '../difference-stream.js';

import {
  Album,
  Artist,
  orderIsRemovedFromReply,
  orderIsRemovedFromRequest,
  Track,
  TrackArtist,
} from './join-operator-test-util.js';

test('left join', () => {
  const trackInput = new DifferenceStream<Track>();
  const albumInput = new DifferenceStream<Album>();

  const output = trackInput.leftJoin(
    {
      aTable: 'track',
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['track', 'albumId'],

      b: albumInput,
      bTable: 'album',
      bAs: 'album',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['album', 'id'],
    },
    undefined,
  );

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

  const trackTrackArtist = trackInput.leftJoin(
    {
      aTable: 'track',
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['track', 'id'],

      b: trackArtistInput,
      bTable: 'trackArtist',
      bAs: 'trackArtist',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['trackArtist', 'trackId'],
    },
    undefined,
  );

  const output = trackTrackArtist.leftJoin(
    {
      aTable: undefined,
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['trackArtist', 'artistId'],

      b: artistInput,
      bTable: 'artist',
      bAs: 'artist',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['artist', 'id'],
    },
    undefined,
  );

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

  const trackTrackArtist = trackInput.leftJoin(
    {
      aTable: 'track',
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['track', 'id'],

      b: trackArtistInput,
      bTable: 'trackArtist',
      bAs: 'trackArtist',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['trackArtist', 'trackId'],
    },
    undefined,
  );

  const output = trackTrackArtist.leftJoin(
    {
      aTable: undefined,
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['trackArtist', 'artistId'],

      b: artistInput,
      bTable: 'artist',
      bAs: 'artist',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['artist', 'id'],
    },
    undefined,
  );

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

  const output = trackInput.leftJoin(
    {
      aTable: 'track',
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['track', 'albumId'],

      b: albumInput,
      bTable: 'album',
      bAs: 'album',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['album', 'id'],
    },
    undefined,
  );

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

  const output = trackInput.leftJoin(
    {
      aTable: 'track',
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['track', 'id'],

      b: trackArtistInput,
      bTable: 'trackArtist',
      bAs: 'trackArtist',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['trackArtist', 'trackId'],
    },
    undefined,
  );

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

  const trackTrackArtist = trackInput.leftJoin(
    {
      aTable: 'track',
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['track', 'id'],

      b: trackArtistInput,
      bTable: 'trackArtist',
      bAs: 'trackArtist',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['trackArtist', 'trackId'],
    },
    undefined,
  );

  const output = trackTrackArtist.leftJoin(
    {
      aTable: undefined,
      aPrimaryKeyColumns: ['id'],
      aJoinColumn: ['trackArtist', 'artistId'],

      b: artistInput,
      bTable: 'artist',
      bAs: 'artist',
      bPrimaryKeyColumns: ['id'],
      bJoinColumn: ['artist', 'id'],
    },
    undefined,
  );

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
