import {describe, expect, test} from 'vitest';
import {and, exp, or} from 'zql/src/zql/query/entity-query.js';
import {singleTableCases} from 'zql/src/zql/prev-next-test-cases.js';
import {bulkSet, musicAppQueries, newZero} from './integration-test-util.js';
import fc from 'fast-check';

describe('complex expressions', async () => {
  const z = newZero(musicAppQueries);
  const tracks = [
    {
      id: '001',
      title: 'a',
      albumId: '001',
      length: 100,
    },
    {
      id: '002',
      title: 'a',
      albumId: '001',
      length: 200,
    },
    {
      id: '003',
      title: 'a',
      albumId: '002',
      length: 300,
    },
    {
      id: '004',
      title: 'c',
      albumId: '002',
      length: 400,
    },
  ];
  await bulkSet(z, {
    tracks,
  });

  // TODO(mlaw): also test these in a way that we can validate the optimized query plan is being run.
  // We need to hook up some way of tracing execution of a query and inspecting the trace.
  test.each([
    {
      name: 'cursor style, asc.',
      // attempt to get the page after id001.
      // The cursor for a pagination after id001 would be the title and id of id001.
      // id002 has a duplicate title of id001 but a higher id so it should be included.
      query: () =>
        z.query.track
          .orderBy('title', 'asc')
          .where(
            or(
              exp('title', '>', 'a'),
              and(exp('title', '=', 'a'), exp('id', '>', '001')),
            ),
          ),
      expected: tracks.slice(1),
    },
    {
      name: 'cursor style, asc, starting from unique (id003)',
      query: () =>
        z.query.track
          .orderBy('title', 'asc')
          .where(
            or(
              exp('title', '>', 'a'),
              and(exp('title', '=', 'a'), exp('id', '>', '003')),
            ),
          ),
      expected: tracks.slice(3),
    },
    {
      name: 'cursor style, desc with dupes',
      // attempt to get the page before id003.
      query: () =>
        z.query.track
          .orderBy('title', 'desc')
          .where(
            or(
              exp('title', '<', 'a'),
              and(exp('title', '=', 'a'), exp('id', '<', '003')),
            ),
          ),
      expected: tracks.slice(0, 2).reverse(),
    },
    {
      name: 'cursor style desc, starting from unique (id004)',
      query: () =>
        z.query.track
          .orderBy('title', 'desc')
          .where(
            or(
              exp('title', '<', 'c'),
              and(exp('title', '=', 'c'), exp('id', '<', '004')),
            ),
          ),
      expected: tracks.slice(0, 3).reverse(),
    },
    {
      name: 'two or equalities',
      query: () =>
        z.query.track.where(or(exp('id', '=', '001'), exp('id', '=', '002'))),
      expected: tracks.slice(0, 2),
    },
    {
      name: 'cursor style with 2 orderings',
      query: () =>
        z.query.track
          .orderBy('title', 'asc')
          .orderBy('albumId', 'asc')
          .where('title', 'IN', ['a', 'c'])
          .where(
            or(
              exp('title', '>', 'a'),
              and(exp('title', '=', 'a'), exp('albumId', '>', '001')),
              and(
                exp('title', '=', 'a'),
                exp('albumId', '=', '001'),
                exp('id', '>', '001'),
              ),
            ),
          )
          .limit(1),
      expected: [tracks[1]],
    },
    {
      name: 'cursor style with 2 orderings, less overlap',
      query: () =>
        z.query.track
          .orderBy('title', 'asc')
          .orderBy('length', 'asc')
          .where(
            or(
              exp('title', '>', 'a'),
              and(exp('title', '=', 'a'), exp('length', '>', 100)),
              and(
                exp('title', '=', 'a'),
                exp('length', '=', 100),
                exp('id', '>', '001'),
              ),
            ),
          )
          .limit(1),
      expected: [tracks[1]],
    },
  ])('$name', async ({query, expected}) => {
    const stmt = query().prepare();
    const rows = await stmt.exec();
    stmt.destroy();

    expect(rows).toEqual(expected);
  });
});

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

test('fast check 3 field order by', async () => {
  await fc.assert(fc.asyncProperty(trackArbitrary, fc.gen(), checkIt));
});

async function checkIt(tracks: readonly Track[], gen: fc.GeneratorValue) {
  const z = newZero(musicAppQueries);
  await bulkSet(z, {
    tracks,
  });

  const index = gen(fc.integer, {min: 0, max: tracks.length - 1});
  const randomTrack = tracks[index];
  const trackQuery = z.query.track;

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
  await z.close();
}

test.each(singleTableCases)('Complex paging - $name', async ({tracks}) => {
  for (let i = 0; i < tracks.length; i++) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await checkIt(tracks, (() => i) as any);
  }
});
