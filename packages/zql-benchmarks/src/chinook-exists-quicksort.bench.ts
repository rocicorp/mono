// Benchmarks for the FlippedJoin quicksort path
// (see packages/zql/src/ivm/flipped-join.ts:#fetchQuicksort).
//
// FlippedJoin is built when a `whereExists` is *flipped*: child is fetched
// first, then each child looks up its (unique) parent. When the join's
// parentKey matches a unique index on the parent (PK or any UNIQUE INDEX),
// every child→parent fetch returns at most one row, so the previous N-way
// merge-sort path opened N simultaneous prepared-statement cursors each
// holding a single-row iterator — wasted setup that also defeats statement
// caching. The quicksort path replaces that with a single sequential loop
// (one prepared statement reused N times) followed by an in-memory sort by
// parent.row.
//
// Each benchmark below picks a relationship where the parent's join key is
// the parent's primary key, so flip:true routes through quicksort. We pair
// each flipped query with its non-flipped (Cap) twin so the two strategies
// can be compared on the same shape.

import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {runBenchmarks} from '../../zql-integration-tests/src/helpers/runner.ts';

const pgContent = await getChinook();

await runBenchmarks(
  {
    suiteName: 'chinook_bench_quicksort',
    type: 'hydration',
    pgContent,
    zqlSchema: schema,
  },
  [
    // album.id is album's PK and the parent of the flipped exists. Each track
    // looks up exactly one album, so quicksort's single-statement reuse pays
    // off once per track.
    {
      name: 'album.exists(tracks long) flip=true (quicksort)',
      createQuery: q =>
        q.album.whereExists(
          'tracks',
          t => t.where('milliseconds', '>', 200_000),
          {flip: true},
        ),
    },
    {
      name: 'album.exists(tracks long) flip=false (cap)',
      createQuery: q =>
        q.album.whereExists(
          'tracks',
          t => t.where('milliseconds', '>', 200_000),
          {flip: false},
        ),
    },

    // artist.id is artist's PK. The track→album→artist chain produces a
    // larger child-side count after filter than album→tracks does, so the
    // flipped path here drives many child rows through one statement-cache
    // entry.
    {
      name: 'artist.exists(albums) flip=true (quicksort)',
      createQuery: q =>
        q.artist.whereExists('albums', a => a.where('title', 'LIKE', '%The%'), {
          flip: true,
        }),
    },
    {
      name: 'artist.exists(albums) flip=false (cap)',
      createQuery: q =>
        q.artist.whereExists('albums', a => a.where('title', 'LIKE', '%The%'), {
          flip: false,
        }),
    },

    // genre.id is genre's PK; only ~25 genres but ~3.5k tracks, so the
    // flipped path collapses many child rows onto a small parent set, which
    // is exactly the shape quicksort's group-by-equal-parent loop targets.
    {
      name: 'genre.exists(tracks) flip=true (quicksort)',
      createQuery: q =>
        q.genre.whereExists(
          'tracks',
          t => t.where('milliseconds', '>', 100_000),
          {flip: true},
        ),
    },
    {
      name: 'genre.exists(tracks) flip=false (cap)',
      createQuery: q =>
        q.genre.whereExists(
          'tracks',
          t => t.where('milliseconds', '>', 100_000),
          {flip: false},
        ),
    },

    // mediaType has only ~5 rows. The flipped path here is the worst case
    // for the *old* merge-sort path — 5 parallel iterators each holding a
    // single-row cursor — so the quicksort delta should be most visible.
    {
      name: 'mediaType.exists(tracks) flip=true (quicksort)',
      createQuery: q =>
        q.mediaType.whereExists(
          'tracks',
          t => t.where('bytes', '>', 1_000_000),
          {
            flip: true,
          },
        ),
    },
    {
      name: 'mediaType.exists(tracks) flip=false (cap)',
      createQuery: q =>
        q.mediaType.whereExists(
          'tracks',
          t => t.where('bytes', '>', 1_000_000),
          {
            flip: false,
          },
        ),
    },
  ],
);
