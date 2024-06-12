import fc from 'fast-check';
import {test, expect} from 'vitest';
import {TestContext} from './context/test-context.js';
import {singleTableCases} from './prev-next-test-cases.js';
import {and, EntityQuery, exp, or} from './query/entity-query.js';
import {
  Artist,
  createRandomAlbums,
  createRandomArtists,
  createRandomTracks,
  linkTracksToArtists,
  Track,
  TrackArtist,
} from './test-helpers/create-data.js';
import * as agg from './query/agg.js';

const trackArbitrary: fc.Arbitrary<Track[]> = fc.array(
  fc.record({
    id: fc.uuid().noShrink(),
    title: fc.string(),
    albumId: fc.string(),
    length: fc.integer(),
  }),
  {
    minLength: 1,
  },
);

test('3 field paging', async () => {
  await fc.assert(fc.asyncProperty(trackArbitrary, fc.gen(), checkSingleTable));
});

test.each(singleTableCases)('3 field paging - $name', async ({tracks}) => {
  for (let i = 0; i < tracks.length; i++) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await checkSingleTable(tracks, (() => i) as any);
  }
});

test('double left join & group by w/ 3 field paging & some overlap', async () => {
  const artists = createRandomArtists(5);
  const albums = createRandomAlbums(5, artists);
  const tracks = createRandomTracks(10, albums, {
    titles: ['a', 'b'],
    lengths: [100, 200, 300],
  });
  const trackArtists = linkTracksToArtists(artists, tracks);
  await checkDoubleLeftJoinGroupBy(tracks, artists, trackArtists);
});

async function checkSingleTable(
  tracks: readonly Track[],
  gen: fc.GeneratorValue,
) {
  const context = new TestContext();
  const trackSource = context.getSource('track');
  context.materialite.tx(() => {
    for (const track of tracks) {
      trackSource.add(track);
    }
  });

  const index = gen(fc.integer, {min: 0, max: tracks.length - 1});
  const randomTrack = tracks[index];
  const trackQuery = new EntityQuery<{track: Track}>(context, 'track');

  const query = trackQuery
    .select('*')
    .orderBy('title', 'asc')
    .orderBy('length', 'asc')
    .where(
      or(
        exp('title', '>', randomTrack.title),
        and(
          exp('title', '=', randomTrack.title),
          exp('length', '>', randomTrack.length),
        ),
        and(
          exp('title', '=', randomTrack.title),
          exp('length', '=', randomTrack.length),
          exp('id', '>', randomTrack.id),
        ),
      ),
    )
    .limit(2);

  const stmt = query.prepare();
  const rows = await stmt.exec();
  stmt.destroy();

  const sortedTracks = tracks.concat().sort(titleLengthIdComparator);

  const sortedTrackIndex = sortedTracks.findIndex(t => t.id === randomTrack.id);
  const nextTwo = sortedTracks.slice(
    sortedTrackIndex + 1,
    sortedTrackIndex + 3,
  );
  expect(rows).toEqual(nextTwo);
}

async function checkDoubleLeftJoinGroupBy(
  tracks: Track[],
  artists: Artist[],
  trackArtists: TrackArtist[],
) {
  const context = new TestContext();
  const trackSource = context.getSource('track');
  const trackArtistSource = context.getSource('trackArtist');
  const artistSource = context.getSource('artist');
  context.materialite.tx(() => {
    for (const track of tracks) {
      trackSource.add(track);
    }
    for (const trackArtist of trackArtists) {
      trackArtistSource.add(trackArtist);
    }
    for (const artist of artists) {
      artistSource.add(artist);
    }
  });

  const trackQuery = new EntityQuery<{track: Track}>(context, 'track');
  const trackArtistQuery = new EntityQuery<{trackArtist: TrackArtist}>(
    context,
    'trackArtist',
  );
  const artistQuery = new EntityQuery<{artist: Artist}>(context, 'artist');

  const trackArtistsIndex = new Map<string, TrackArtist[]>();
  for (const trackArtist of trackArtists) {
    let existing = trackArtistsIndex.get(trackArtist.trackId);
    if (!existing) {
      existing = [];
      trackArtistsIndex.set(trackArtist.trackId, existing);
    }
    existing.push(trackArtist);
  }
  const expectedResult = tracks
    .map(t => ({
      track: t,
      artists: (
        trackArtistsIndex
          .get(t.id)
          ?.map(ta => artists.find(a => a.id === ta.artistId))
          .filter((a): a is Artist => a !== undefined) ?? []
      ).sort((l, r) => {
        if (l.id > r.id) {
          return 1;
        } else if (l.id < r.id) {
          return -1;
        }
        return 0;
      }),
    }))
    .sort(titleLengthIdComparatorFromJoinResult);

  for (let cursorPointer = 0; cursorPointer < tracks.length; ++cursorPointer) {
    const cursorTrack = tracks[cursorPointer];
    const query = trackQuery
      .leftJoin(
        trackArtistQuery,
        'trackArtist',
        'track.id',
        'trackArtist.trackId',
      )
      .leftJoin(artistQuery, 'artist', 'trackArtist.artistId', 'artist.id')
      .select('track.*', agg.array('artist.*', 'artists'))
      .orderBy('track.title', 'asc')
      .orderBy('track.length', 'asc')
      .groupBy('track.id')
      .where(
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore see: https://www.notion.so/replicache/Types-break-for-complex-where-conditions-on-left-joined-queries-a1e7ce550d224ff2aa9f2b60f8f7e5a9?pvs=4
        or(
          exp('track.title', '>', cursorTrack.title),
          and(
            exp('track.title', '=', cursorTrack.title),
            exp('track.length', '>', cursorTrack.length),
          ),
          and(
            exp('track.title', '=', cursorTrack.title),
            exp('track.length', '=', cursorTrack.length),
            exp('track.id', '>', cursorTrack.id),
          ),
        ),
      )
      .limit(2);
    const stmt = query.prepare();
    const rows = await stmt.exec();
    stmt.destroy();
    const allStmt = await trackQuery
      .leftJoin(
        trackArtistQuery,
        'trackArtist',
        'track.id',
        'trackArtist.trackId',
      )
      .leftJoin(artistQuery, 'artist', 'trackArtist.artistId', 'artist.id')
      .select('track.*', agg.array('artist.*', 'artists'))
      .orderBy('track.title', 'asc')
      .orderBy('track.length', 'asc')
      .groupBy('track.id')
      .prepare();
    const allRows = await allStmt.exec();
    allStmt.destroy();

    const sortedTrackIndex = expectedResult.findIndex(
      t => t.track.id === cursorTrack.id,
    );
    const nextTwo = expectedResult.slice(
      sortedTrackIndex + 1,
      sortedTrackIndex + 3,
    );

    // See if the non-filtered result matches.
    // Maybe bug is in order-by not filter.
    expect(
      allRows.map(r => ({
        track: r.track,
        artists: r.artists,
      })),
    ).toEqual(expectedResult);

    expect(
      rows.map(t => ({
        track: t.track,
        artists: t.artists,
      })),
    ).toEqual(nextTwo);
  }
}

const titleLengthIdComparator = (a: Track, b: Track) => {
  if (a.title < b.title) {
    return -1;
  }
  if (a.title > b.title) {
    return 1;
  }
  if (a.length < b.length) {
    return -1;
  }
  if (a.length > b.length) {
    return 1;
  }
  if (a.id < b.id) {
    return -1;
  }
  if (a.id > b.id) {
    return 1;
  }
  return 0;
};

const titleLengthIdComparatorFromJoinResult = (
  a: {track: Track},
  b: {track: Track},
) => titleLengthIdComparator(a.track, b.track);
