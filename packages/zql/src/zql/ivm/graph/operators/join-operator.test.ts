import {expect, test} from 'vitest';
import {JoinResult, joinSymbol} from '../../types.js';
import {DifferenceStream} from '../difference-stream.js';

type Track = {
  id: number;
  title: string;
  length: number;
  albumId: number;
};

type Album = {
  id: number;
  title: string;
};

type TrackArtist = {
  trackId: number;
  artistId: number;
};

type Artist = {
  id: number;
  name: string;
};

test('unbalanced input', () => {
  const trackInput = new DifferenceStream<Track>();
  const albumInput = new DifferenceStream<Album>();

  const output = trackInput.join({
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

  trackInput.newDifference(1, [
    [
      {
        id: 1,
        title: 'Track One',
        length: 1,
        albumId: 1,
      },
      1,
    ],
  ]);

  trackInput.commit(1);
  expect(items).toEqual([]);
  items.length = 0;

  // now add to the other side
  albumInput.newDifference(2, [
    [
      {
        id: 1,
        title: 'Album One',
      },
      1,
    ],
  ]);

  albumInput.commit(2);
  expect(items).toEqual([
    [
      {
        id: '1_1',
        [joinSymbol]: true,
        track: {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        album: {
          id: 1,
          title: 'Album One',
        },
      },
      1,
    ],
  ]);
  items.length = 0;

  // now try deleting items
  albumInput.newDifference(3, [
    [
      {
        id: 1,
        title: 'Album One',
      },
      -1,
    ],
  ]);
  albumInput.commit(3);
  // Join result is retracted and no new output is produced
  // since this is an inner join, not left join.
  expect(items).toEqual([
    [
      {
        id: '1_1',
        [joinSymbol]: true,
        track: {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        album: {
          id: 1,
          title: 'Album One',
        },
      },
      -1,
    ],
  ]);
  items.length = 0;

  // add it back
  albumInput.newDifference(4, [
    [
      {
        id: 1,
        title: 'Album One',
      },
      1,
    ],
  ]);
  albumInput.commit(4);

  expect(items).toEqual([
    [
      {
        id: '1_1',
        [joinSymbol]: true,
        track: {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        album: {
          id: 1,
          title: 'Album One',
        },
      },
      1,
    ],
  ]);
  items.length = 0;
});

test('basic join', () => {
  const trackInput = new DifferenceStream<Track>();
  const albumInput = new DifferenceStream<Album>();

  const output = trackInput.join({
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

  trackInput.newDifference(1, [
    [
      {
        id: 1,
        title: 'Track One',
        length: 1,
        albumId: 1,
      },
      1,
    ],
  ]);

  albumInput.newDifference(1, [
    [
      {
        id: 1,
        title: 'Album One',
      },
      1,
    ],
  ]);

  check([
    [
      {
        id: '1_1',
        [joinSymbol]: true,
        track: {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        album: {
          id: 1,
          title: 'Album One',
        },
      },
      1,
    ],
  ]);

  function check(expected: [unknown, number][]) {
    albumInput.commit(2);
    expect(items).toEqual(expected);
    items.length = 0;
  }
});

test('join through a junction table', () => {
  // track -> track_artist -> artist
  let version = 0;
  const trackInput = new DifferenceStream<Track>();
  const trackArtistInput = new DifferenceStream<TrackArtist>();
  const artistInput = new DifferenceStream<Artist>();

  const trackAndTrackArtistOutput = trackInput.join({
    aAs: 'track',
    getAJoinKey: track => track.id,
    getAPrimaryKey: track => track.id,
    b: trackArtistInput,
    bAs: 'trackArtist',
    getBJoinKey: trackArtist => trackArtist.trackId,
    getBPrimaryKey: trackArtist =>
      trackArtist.trackId + '-' + trackArtist.artistId,
  });

  const output = trackAndTrackArtistOutput.join({
    aAs: undefined,
    getAJoinKey: x => x.trackArtist.artistId,
    getAPrimaryKey: x => x.id,
    b: artistInput,
    bAs: 'artist',
    getBJoinKey: x => x.id,
    getBPrimaryKey: x => x.id,
  });

  const items: [
    JoinResult<Track, TrackArtist, 'track', 'trackArtist'>,
    number,
  ][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  ++version;
  trackInput.newDifference(version, [
    [
      {
        id: 1,
        title: 'Track One',
        length: 1,
        albumId: 1,
      },
      1,
    ],
  ]);
  trackArtistInput.newDifference(version, [
    [
      {
        trackId: 1,
        artistId: 1,
      },
      1,
    ],
    [
      {
        trackId: 1,
        artistId: 2,
      },
      1,
    ],
  ]);
  artistInput.newDifference(version, [
    [
      {
        id: 1,
        name: 'Artist One',
      },
      1,
    ],
    [
      {
        id: 2,
        name: 'Artist Two',
      },
      1,
    ],
  ]);

  trackInput.commit(version);
  trackArtistInput.commit(version);
  artistInput.commit(version);

  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: 1, title: 'Track One', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 1},
        artist: {id: 1, name: 'Artist One'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1_1-2_2',
        track: {id: 1, title: 'Track One', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 2},
        artist: {id: 2, name: 'Artist Two'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
  items.length = 0;

  // remove an artist
  ++version;
  artistInput.newDifference(version, [
    [
      {
        id: 2,
        name: 'Artist Two',
      },
      -1,
    ],
  ]);
  artistInput.commit(version);

  // artist-two row is retracted
  expect(items).toEqual([
    [
      {
        id: '1_1-2_2',
        track: {id: 1, title: 'Track One', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 2},
        artist: {id: 2, name: 'Artist Two'},
        [joinSymbol]: true,
      },
      -1,
    ],
  ]);
  items.length = 0;

  // remove a track-artist link
  ++version;
  trackArtistInput.newDifference(version, [
    [
      {
        trackId: 1,
        artistId: 2,
      },
      -1,
    ],
  ]);
  trackArtistInput.commit(version);

  // should be no output -- the track-artist link is gone
  expect(items).toEqual([]);
  items.length = 0;

  // remove the track
  ++version;
  trackInput.newDifference(version, [
    [
      {
        id: 1,
        title: 'Track One',
        length: 1,
        albumId: 1,
      },
      -1,
    ],
  ]);
  trackInput.commit(version);

  // all rows are retracted
  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: 1, title: 'Track One', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 1},
        artist: {id: 1, name: 'Artist One'},
        [joinSymbol]: true,
      },
      -1,
    ],
  ]);
  items.length = 0;

  // remove remaining track-artist link
  ++version;
  trackArtistInput.newDifference(version, [
    [
      {
        trackId: 1,
        artistId: 1,
      },
      -1,
    ],
  ]);
  // remove remaining artist
  artistInput.newDifference(version, [
    [
      {
        id: 1,
        name: 'Artist One',
      },
      -1,
    ],
  ]);
  trackArtistInput.commit(version);
  artistInput.commit(version);

  // all rows are retracted -> 0
  expect(items).toEqual([]);
  items.length = 0;

  ++version;
  artistInput.newDifference(version, [
    [
      {
        id: 1,
        name: 'Artist A',
      },
      1,
    ],
  ]);
  artistInput.newDifference(version, [
    [
      {
        id: 2,
        name: 'Artist B',
      },
      1,
    ],
  ]);
  trackInput.newDifference(version, [
    [
      {
        id: 1,
        title: 'Track A',
        length: 1,
        albumId: 1,
      },
      1,
    ],
  ]);
  trackInput.newDifference(version, [
    [
      {
        id: 2,
        title: 'Track B',
        length: 1,
        albumId: 1,
      },
      1,
    ],
  ]);
  trackArtistInput.newDifference(version, [
    [
      {
        trackId: 1,
        artistId: 1,
      },
      1,
    ],
  ]);
  trackArtistInput.newDifference(version, [
    [
      {
        trackId: 2,
        artistId: 2,
      },
      1,
    ],
  ]);
  artistInput.commit(version);
  trackInput.commit(version);
  trackArtistInput.commit(version);

  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: 1, title: 'Track A', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 1},
        artist: {id: 1, name: 'Artist A'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '2_2_2-2',
        track: {id: 2, title: 'Track B', length: 1, albumId: 1},
        trackArtist: {trackId: 2, artistId: 2},
        artist: {id: 2, name: 'Artist B'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
});

test('add many items to the same source as separate calls in the same tick', () => {
  let version = 0;
  const trackInput = new DifferenceStream<Track>();
  const trackArtistInput = new DifferenceStream<TrackArtist>();
  const artistInput = new DifferenceStream<Artist>();

  const trackAndTrackArtistOutput = trackInput.join({
    aAs: 'track',
    getAJoinKey: track => track.id,
    getAPrimaryKey: track => track.id,
    b: trackArtistInput,
    bAs: 'trackArtist',
    getBJoinKey: trackArtist => trackArtist.trackId,
    getBPrimaryKey: trackArtist =>
      trackArtist.trackId + '-' + trackArtist.artistId,
  });

  const output = trackAndTrackArtistOutput.join({
    aAs: undefined,
    getAJoinKey: x => x.trackArtist.artistId,
    getAPrimaryKey: x => x.id,
    b: artistInput,
    bAs: 'artist',
    getBJoinKey: x => x.id,
    getBPrimaryKey: x => x.id,
  });

  const items: [
    JoinResult<Track, TrackArtist, 'track', 'trackArtist'>,
    number,
  ][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  // add some artists
  ++version;
  artistInput.newDifference(version, [
    [
      {
        id: 1,
        name: 'Artist A',
      },
      1,
    ],
  ]);
  artistInput.newDifference(version, [
    [
      {
        id: 2,
        name: 'Artist B',
      },
      1,
    ],
  ]);
  trackInput.newDifference(version, [
    [
      {
        id: 1,
        title: 'Track A',
        length: 1,
        albumId: 1,
      },
      1,
    ],
  ]);
  trackInput.newDifference(version, [
    [
      {
        id: 2,
        title: 'Track B',
        length: 1,
        albumId: 1,
      },
      1,
    ],
  ]);
  trackArtistInput.newDifference(version, [
    [
      {
        trackId: 1,
        artistId: 1,
      },
      1,
    ],
  ]);
  trackArtistInput.newDifference(version, [
    [
      {
        trackId: 2,
        artistId: 2,
      },
      1,
    ],
  ]);
  artistInput.commit(version);
  trackInput.commit(version);
  trackArtistInput.commit(version);

  expect(items).toEqual([
    [
      {
        id: '1_1_1-1',
        track: {id: 1, title: 'Track A', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 1},
        artist: {id: 1, name: 'Artist A'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '2_2_2-2',
        track: {id: 2, title: 'Track B', length: 1, albumId: 1},
        trackArtist: {trackId: 2, artistId: 2},
        artist: {id: 2, name: 'Artist B'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
});
