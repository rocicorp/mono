import fc from 'fast-check';
import {test, expect} from 'vitest';
import {TestContext} from './context/test-context.js';
import {singleTableCases} from './prev-next-test-cases.js';
import {and, EntityQuery, exp, or} from './query/entity-query.js';
import type {Track} from './test-helpers/create-data.js';

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
