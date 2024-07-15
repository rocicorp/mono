import {expect, test} from 'vitest';
import {JoinResult, StringOrNumber, joinSymbol} from '../../types.js';
import {DifferenceStream} from '../difference-stream.js';
import {
  orderIsRemovedFromReply,
  orderIsRemovedFromRequest,
} from './join-operator-test-util.js';
import {makeJoinResult} from './join-operator.js';

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
    aTable: 'track',
    aPrimaryKeyColumns: ['id'],
    aJoinColumn: ['track', 'albumId'],

    b: albumInput,
    bTable: 'album',
    bAs: 'album',
    bPrimaryKeyColumns: ['id'],
    bJoinColumn: ['album', 'id'],
  });

  const items: [JoinResult<Track, Album, 'track', 'album'>, number][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  trackInput.newDifference(
    1,
    [
      [
        {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        1,
      ],
    ],
    undefined,
  );

  trackInput.commit(1);
  expect(items).toEqual([]);
  items.length = 0;

  // now add to the other side
  albumInput.newDifference(
    2,
    [
      [
        {
          id: 1,
          title: 'Album One',
        },
        1,
      ],
    ],
    undefined,
  );

  albumInput.commit(2);
  expect(items).toEqual([
    [
      {
        id: '1:1',
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
  albumInput.newDifference(
    3,
    [
      [
        {
          id: 1,
          title: 'Album One',
        },
        -1,
      ],
    ],
    undefined,
  );
  albumInput.commit(3);
  // Join result is retracted and no new output is produced
  // since this is an inner join, not left join.
  expect(items).toEqual([
    [
      {
        id: '1:1',
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
  albumInput.newDifference(
    4,
    [
      [
        {
          id: 1,
          title: 'Album One',
        },
        1,
      ],
    ],
    undefined,
  );
  albumInput.commit(4);

  expect(items).toEqual([
    [
      {
        id: '1:1',
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
    aTable: 'track',
    aPrimaryKeyColumns: ['id'],
    aJoinColumn: ['track', 'albumId'],

    b: albumInput,
    bTable: 'album',
    bAs: 'album',
    bPrimaryKeyColumns: ['id'],
    bJoinColumn: ['album', 'id'],
  });

  const items: [JoinResult<Track, Album, 'track', 'album'>, number][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  trackInput.newDifference(
    1,
    [
      [
        {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        1,
      ],
    ],
    undefined,
  );

  albumInput.newDifference(
    1,
    [
      [
        {
          id: 1,
          title: 'Album One',
        },
        1,
      ],
    ],
    undefined,
  );

  check([
    [
      {
        id: '1:1',
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

  // track x trackartist
  const trackAndTrackArtistOutput = trackInput.join({
    aTable: 'track',
    aPrimaryKeyColumns: ['id'],
    aJoinColumn: ['track', 'id'],

    b: trackArtistInput,
    bTable: 'trackArtist',
    bAs: 'trackArtist',
    bPrimaryKeyColumns: ['trackId', 'artistId'],
    bJoinColumn: ['trackArtist', 'trackId'],
  });

  // (track x trackartist) x artist
  const output = trackAndTrackArtistOutput.join({
    aTable: undefined,
    aPrimaryKeyColumns: ['id'],
    aJoinColumn: ['trackArtist', 'artistId'],

    b: artistInput,
    bTable: 'artist',
    bAs: 'artist',
    bPrimaryKeyColumns: ['id'],
    bJoinColumn: ['artist', 'id'],
  });

  const items: [
    JoinResult<Track, TrackArtist, 'track', 'trackArtist'>,
    number,
  ][] = [];
  output.effect((e, m) => {
    items.push([e, m]);
  });

  ++version;
  trackInput.newDifference(
    version,
    [
      [
        {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
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
    ],
    undefined,
  );
  artistInput.newDifference(
    version,
    [
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
    ],
    undefined,
  );

  trackInput.commit(version);
  trackArtistInput.commit(version);
  artistInput.commit(version);

  expect(items).toEqual([
    [
      {
        id: '1:1-1:1',
        track: {id: 1, title: 'Track One', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 1},
        artist: {id: 1, name: 'Artist One'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '1:1-2:2',
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
  artistInput.newDifference(
    version,
    [
      [
        {
          id: 2,
          name: 'Artist Two',
        },
        -1,
      ],
    ],
    undefined,
  );
  artistInput.commit(version);

  // artist-two row is retracted
  expect(items).toEqual([
    [
      {
        id: '1:1-2:2',
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
  trackArtistInput.newDifference(
    version,
    [
      [
        {
          trackId: 1,
          artistId: 2,
        },
        -1,
      ],
    ],
    undefined,
  );
  trackArtistInput.commit(version);

  // should be no output -- the track-artist link is gone
  expect(items).toEqual([]);
  items.length = 0;

  // remove the track
  ++version;
  trackInput.newDifference(
    version,
    [
      [
        {
          id: 1,
          title: 'Track One',
          length: 1,
          albumId: 1,
        },
        -1,
      ],
    ],
    undefined,
  );
  trackInput.commit(version);

  // all rows are retracted
  expect(items).toEqual([
    [
      {
        id: '1:1-1:1',
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
  trackArtistInput.newDifference(
    version,
    [
      [
        {
          trackId: 1,
          artistId: 1,
        },
        -1,
      ],
    ],
    undefined,
  );
  // remove remaining artist
  artistInput.newDifference(
    version,
    [
      [
        {
          id: 1,
          name: 'Artist One',
        },
        -1,
      ],
    ],
    undefined,
  );
  trackArtistInput.commit(version);
  artistInput.commit(version);

  // all rows are retracted -> 0
  expect(items).toEqual([]);
  items.length = 0;

  ++version;
  artistInput.newDifference(
    version,
    [
      [
        {
          id: 1,
          name: 'Artist A',
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
          id: 2,
          name: 'Artist B',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.newDifference(
    version,
    [
      [
        {
          id: 1,
          title: 'Track A',
          length: 1,
          albumId: 1,
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.newDifference(
    version,
    [
      [
        {
          id: 2,
          title: 'Track B',
          length: 1,
          albumId: 1,
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
          trackId: 1,
          artistId: 1,
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
          trackId: 2,
          artistId: 2,
        },
        1,
      ],
    ],
    undefined,
  );
  artistInput.commit(version);
  trackInput.commit(version);
  trackArtistInput.commit(version);

  expect(items).toEqual([
    [
      {
        id: '1:1-1:1',
        track: {id: 1, title: 'Track A', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 1},
        artist: {id: 1, name: 'Artist A'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '2:2-2:2',
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
    aTable: 'track',
    aPrimaryKeyColumns: ['id'],
    aJoinColumn: ['track', 'id'],

    b: trackArtistInput,
    bTable: 'trackArtist',
    bAs: 'trackArtist',
    bPrimaryKeyColumns: ['trackId', 'artistId'],
    bJoinColumn: ['trackArtist', 'trackId'],
  });

  const output = trackAndTrackArtistOutput.join({
    aTable: undefined,
    aPrimaryKeyColumns: ['id'],
    aJoinColumn: ['trackArtist', 'artistId'],

    b: artistInput,
    bTable: 'artist',
    bAs: 'artist',
    bPrimaryKeyColumns: ['id'],
    bJoinColumn: ['artist', 'id'],
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
  artistInput.newDifference(
    version,
    [
      [
        {
          id: 1,
          name: 'Artist A',
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
          id: 2,
          name: 'Artist B',
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.newDifference(
    version,
    [
      [
        {
          id: 1,
          title: 'Track A',
          length: 1,
          albumId: 1,
        },
        1,
      ],
    ],
    undefined,
  );
  trackInput.newDifference(
    version,
    [
      [
        {
          id: 2,
          title: 'Track B',
          length: 1,
          albumId: 1,
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
          trackId: 1,
          artistId: 1,
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
          trackId: 2,
          artistId: 2,
        },
        1,
      ],
    ],
    undefined,
  );
  artistInput.commit(version);
  trackInput.commit(version);
  trackArtistInput.commit(version);

  expect(items).toEqual([
    [
      {
        id: '1:1-1:1',
        track: {id: 1, title: 'Track A', length: 1, albumId: 1},
        trackArtist: {trackId: 1, artistId: 1},
        artist: {id: 1, name: 'Artist A'},
        [joinSymbol]: true,
      },
      1,
    ],
    [
      {
        id: '2:2-2:2',
        track: {id: 2, title: 'Track B', length: 1, albumId: 1},
        trackArtist: {trackId: 2, artistId: 2},
        artist: {id: 2, name: 'Artist B'},
        [joinSymbol]: true,
      },
      1,
    ],
  ]);
});

test('order is removed from request', () => {
  orderIsRemovedFromRequest('join');
});

test('order is removed from reply', () => {
  orderIsRemovedFromReply('join');
});

test('makeJoinResult', () => {
  const cases: {
    description: string;
    aVal: Record<string, unknown>;
    bVal: Record<string, unknown> | undefined;
    aAlias: string;
    bAlias: string | undefined;
    getAID: (value: Record<string, unknown>) => StringOrNumber;
    getBID: (value: Record<string, unknown>) => StringOrNumber;
    expected: unknown;
  }[] = [
    {
      description: 'row x undefined',
      aVal: {id1: 1, val1: 'a'},
      bVal: undefined,
      aAlias: 't1',
      bAlias: undefined,
      getAID: v => v.id1 as number,
      getBID: v => v.id2 as number,
      expected: {
        id: '1:',
        t1: {id1: 1, val1: 'a'},
        [joinSymbol]: true,
      },
    },
    {
      description: 'row x row',
      aVal: {id1: 1, val1: 1},
      bVal: {id2: 2, val2: 2},
      aAlias: 't1',
      bAlias: 't2',
      getAID: v => v.id1 as number,
      getBID: v => v.id2 as number,
      expected: {
        id: '1:2',
        t1: {id1: 1, val1: 1},
        t2: {id2: 2, val2: 2},
        [joinSymbol]: true,
      },
    },
    {
      description: 'row x row - reversed id order',
      aVal: {id1: 4, val1: 1},
      bVal: {id2: 3, val2: 2},
      aAlias: 't1',
      bAlias: 't2',
      getAID: v => v.id1 as string,
      getBID: v => v.id2 as string,
      expected: {
        id: '4:3',
        t1: {id1: 4, val1: 1},
        t2: {id2: 3, val2: 2},
        [joinSymbol]: true,
      },
    },
    {
      description: 'row x row - string ids',
      aVal: {id1: '1', val1: 1},
      bVal: {id2: '2', val2: 2},
      aAlias: 't1',
      bAlias: 't2',
      getAID: v => v.id1 as string,
      getBID: v => v.id2 as string,
      expected: {
        id: '1:2',
        t1: {id1: '1', val1: 1},
        t2: {id2: '2', val2: 2},
        [joinSymbol]: true,
      },
    },
    {
      description: 'row x row - badchars',
      aVal: {id1: '1:1%?', val1: 1},
      bVal: {id2: '2:2%?', val2: 2},
      aAlias: 't1',
      bAlias: 't2',
      getAID: v => v.id1 as string,
      getBID: v => v.id2 as string,
      expected: {
        id: '1%3A1%25%3F:2%3A2%25%3F',
        t1: {id1: '1:1%?', val1: 1},
        t2: {id2: '2:2%?', val2: 2},
        [joinSymbol]: true,
      },
    },
    {
      description: 'joinresult x undefined',
      aVal: makeJoinResult(
        {id1: 1, val1: 1},
        {id2: 2, val2: 2},
        't1',
        't2',
        v => v.id1,
        v => v.id2,
      ),
      bVal: undefined,
      aAlias: '',
      bAlias: undefined,
      getAID: v => v.id as string,
      getBID: () => '',
      expected: {
        id: '1:2:',
        t1: {
          id1: 1,
          val1: 1,
        },
        t2: {
          id2: 2,
          val2: 2,
        },
        [joinSymbol]: true,
      },
    },
    {
      description: 'joinresult x undefined (badchars)',
      aVal: makeJoinResult(
        {id1: '1:1%?', val1: 1},
        {id2: '2:2%?', val2: 2},
        't1',
        't2',
        v => v.id1,
        v => v.id2,
      ),
      bVal: undefined,
      aAlias: '',
      bAlias: undefined,
      getAID: v => v.id as string,
      getBID: () => '',
      expected: {
        id: '1%3A1%25%3F:2%3A2%25%3F:',
        t1: {
          id1: '1:1%?',
          val1: 1,
        },
        t2: {
          id2: '2:2%?',
          val2: 2,
        },
        [joinSymbol]: true,
      },
    },
    {
      description: 'join result x row',
      aVal: makeJoinResult(
        {id1: 1, val1: 1},
        {id2: 2, val2: 2},
        't1',
        't2',
        v => v.id1,
        v => v.id2,
      ),
      bVal: {
        id3: 3,
        val3: 3,
      },
      aAlias: 't1',
      bAlias: 't3',
      getAID: v => v.id as string,
      getBID: v => v.id3 as number,
      expected: {
        id: '1:2:3',
        t1: {
          id1: 1,
          val1: 1,
        },
        t2: {
          id2: 2,
          val2: 2,
        },
        t3: {
          id3: 3,
          val3: 3,
        },
        [joinSymbol]: true,
      },
    },
    {
      description: 'join result (with undefined) x row',
      aVal: makeJoinResult(
        {id1: 1, val1: 1},
        undefined,
        't1',
        undefined,
        v => v.id1,
        () => '',
      ),
      bVal: {
        id3: 3,
        val3: 3,
      },
      aAlias: 't1',
      bAlias: 't3',
      getAID: v => v.id as string,
      getBID: v => v.id3 as number,
      expected: {
        id: '1::3',
        t1: {
          id1: 1,
          val1: 1,
        },
        t3: {
          id3: 3,
          val3: 3,
        },
        [joinSymbol]: true,
      },
    },
    {
      description: 'join result (with undefined) x undefined',
      aVal: makeJoinResult(
        {id1: 1, val1: 1},
        undefined,
        't1',
        undefined,
        v => v.id1,
        () => '',
      ),
      bVal: undefined,
      aAlias: 't1',
      bAlias: undefined,
      getAID: v => v.id as string,
      getBID: () => '',
      expected: {
        id: '1::',
        t1: {
          id1: 1,
          val1: 1,
        },
        [joinSymbol]: true,
      },
    },
    {
      description: 'join result x join result',
      aVal: makeJoinResult(
        {id1: 1, val1: 1},
        {id2: 2, val2: 2},
        't1',
        't2',
        v => v.id1,
        v => v.id2,
      ),
      bVal: makeJoinResult(
        {id3: 3, val3: 3},
        {id4: 4, val4: 4},
        't3',
        't4',
        v => v.id3,
        v => v.id4,
      ),
      aAlias: '',
      bAlias: '',
      getAID: v => v.id as string,
      getBID: v => v.id as string,
      expected: {
        id: '1:2:3:4',
        t1: {
          id1: 1,
          val1: 1,
        },
        t2: {
          id2: 2,
          val2: 2,
        },
        t3: {
          id3: 3,
          val3: 3,
        },
        t4: {
          id4: 4,
          val4: 4,
        },
        [joinSymbol]: true,
      },
    },
  ];

  for (const c of cases) {
    const result = makeJoinResult(
      c.aVal,
      c.bVal,
      c.aAlias,
      c.bAlias,
      c.getAID,
      c.getBID,
    );
    expect(result, c.description).toEqual(c.expected);
  }
});
