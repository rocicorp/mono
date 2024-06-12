import fc from 'fast-check';
import {test, expect} from 'vitest';
import {TestContext} from './context/test-context.js';
import {and, EntityQuery, exp, or} from './query/entity-query.js';

type Track = {
  id: string;
  title: string;
  albumId: string;
  length: number;
};

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

test('complex expressions', async () => {
  await fc.assert(fc.asyncProperty(trackArbitrary, fc.gen(), checkIt));
});

async function checkIt(tracks: Track[], gen: fc.GeneratorValue) {
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

  const sortedTracks = tracks.concat().sort((a, b) => {
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
  });

  const sortedTrackIndex = sortedTracks.findIndex(t => t.id === randomTrack.id);
  const nextTwo = sortedTracks.slice(
    sortedTrackIndex + 1,
    sortedTrackIndex + 3,
  );
  expect(rows).toEqual(nextTwo);
}
