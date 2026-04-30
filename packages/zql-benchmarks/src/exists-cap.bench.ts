// Benchmarks for the Cap operator on non-flipped EXISTS subqueries
// (see packages/zql/src/ivm/cap.ts and the wiring in builder.ts).
//
// Why these queries:
//   ZQL completes every Source ordering with the table's primary key. Before
//   Cap, an EXISTS child pipeline was capped with `Take`, which requires a
//   full ordering — so the SQL emitted to SQLite was `... WHERE fk = ?
//   ORDER BY pk LIMIT N`. Whenever the available index didn't already sort
//   by pk under the fk lookup (which is the common case — single-column FK
//   indexes are the norm; compound (fk, pk) indexes are rare), SQLite would
//   either ignore the FK index in favor of a PK scan, or use the FK index
//   and sort the matched rows through a temp b-tree. Both options scan more
//   than EXISTS actually needs.
//
//   Cap removes the ORDER BY by tracking membership via a PK set rather
//   than a sorted bound. SQLite is then free to use the FK index, stop at
//   the first matching row, and never build a temp b-tree.
//
// We can't A/B compare in this benchmark — Cap is unconditional for
// non-flipped EXISTS now — but each case below is a shape that previously
// caused either a temp-b-tree sort or an unnecessary scan, so the absolute
// numbers here are the post-fix ceiling for those shapes. Compare against
// pre-Cap revisions if a regression is suspected.

import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {schema as chinookSchema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {runBenchmarks} from '../../zql-integration-tests/src/helpers/runner.ts';
import {getPagila} from '../../zql-integration-tests/src/pagila/get-deps.ts';
import {schema as pagilaSchema} from '../../zql-integration-tests/src/pagila/schema.ts';

// ---------------------------------------------------------------------------
// Chinook: simpler shapes for isolating individual Cap behaviors.
// ---------------------------------------------------------------------------
{
  const pgContent = await getChinook();

  await runBenchmarks(
    {
      suiteName: 'chinook_bench_cap',
      type: 'hydration',
      pgContent,
      zqlSchema: chinookSchema,
    },
    [
      // Plain FK exists. Index on album.artist_id, but no compound (artist_id,
      // album_id). Pre-Cap → ORDER BY album_id forces a sort or PK scan.
      {
        name: 'artist.exists(albums) — FK lookup, no compound index',
        createQuery: q => q.artist.whereExists('albums', {flip: false}),
      },

      // Filter on a non-indexed column (track.milliseconds). Pre-Cap →
      // SQLite must materialize all matching rows then sort by track_id.
      {
        name: 'album.exists(tracks long) — filter on unindexed column',
        createQuery: q =>
          q.album.whereExists(
            'tracks',
            t => t.where('milliseconds', '>', 200_000),
            {flip: false},
          ),
      },

      // Two ANDed EXISTS — Cap fires twice independently per parent row.
      {
        name: 'track.exists(album).exists(genre)',
        createQuery: q =>
          q.track
            .whereExists('album', {flip: false})
            .whereExists('genre', {flip: false}),
      },

      // Nested EXISTS: outer is a Cap whose child pipeline contains another
      // Cap. Both must be efficient for the whole thing to be efficient.
      {
        name: 'invoiceLine.exists(invoice.exists(customer.country=USA))',
        createQuery: q =>
          q.invoiceLine.whereExists(
            'invoice',
            i =>
              i.whereExists('customer', c => c.where('country', 'USA'), {
                flip: false,
              }),
            {flip: false},
          ),
      },

      // OR over two EXISTS branches. Each branch builds its own Cap; UnionFanIn
      // dedupes. Both branches need to terminate quickly.
      {
        name: 'track.where(exists(album.title) OR exists(genre.name))',
        createQuery: q =>
          q.track.where(({or, exists}) =>
            or(
              exists('album', a => a.where('title', 'LIKE', '%The%'), {
                flip: false,
              }),
              exists('genre', g => g.where('name', 'Rock'), {flip: false}),
            ),
          ),
      },

      // Many-to-many EXISTS through a junction table. Two correlated
      // subqueries are stitched together; Cap applies at the inner edge.
      {
        name: 'playlist.exists(tracks) — through playlist_track junction',
        createQuery: q => q.playlist.whereExists('tracks', {flip: false}),
      },
    ],
  );
}

// ---------------------------------------------------------------------------
// Pagila: same shapes against the larger dataset (16k rentals, 5k film_actor)
// where temp-b-tree costs were most visible historically.
// ---------------------------------------------------------------------------
{
  const pgContent = await getPagila();

  await runBenchmarks(
    {
      suiteName: 'pagila_bench_cap',
      type: 'hydration',
      pgContent,
      zqlSchema: pagilaSchema,
    },
    [
      // Plain FK exists on the largest table in the dataset.
      {
        name: 'customer.exists(rentals)',
        createQuery: q => q.customer.whereExists('rentals', {flip: false}),
      },

      // 4-level chain: each inner level is its own Cap.
      {
        name: 'customer.exists(address.city.country=USA) limit 50',
        createQuery: q =>
          q.customer
            .whereExists(
              'address',
              a =>
                a.whereExists(
                  'city',
                  c =>
                    c.whereExists(
                      'country',
                      co => co.where('country', 'United States'),
                      {flip: false},
                    ),
                  {flip: false},
                ),
              {flip: false},
            )
            .limit(50),
      },

      // Junction-table EXISTS with a filter on the far side.
      {
        name: 'film.exists(actors.lastName="GUINESS")',
        createQuery: q =>
          q.film.whereExists('actors', a => a.where('lastName', 'GUINESS'), {
            flip: false,
          }),
      },

      // rental → inventory → film with a filter on film. Without Cap, every
      // inventory row would force a sort by inventory.id even though only
      // existence is needed.
      {
        name: 'rental.exists(inventory.exists(film.title="ACADEMY DINOSAUR"))',
        createQuery: q =>
          q.rental.whereExists(
            'inventory',
            i =>
              i.whereExists('film', f => f.where('title', 'ACADEMY DINOSAUR'), {
                flip: false,
              }),
            {flip: false},
          ),
      },

      // OR of two EXISTS over different relationships — both branches need
      // to be fast.
      {
        name: 'film.where(exists(actors.lastName=GUINESS) OR exists(categories.name=Action))',
        createQuery: q =>
          q.film.where(({or, exists}) =>
            or(
              exists('actors', a => a.where('lastName', 'GUINESS'), {
                flip: false,
              }),
              exists('categories', c => c.where('name', 'Action'), {
                flip: false,
              }),
            ),
          ),
      },
    ],
  );
}
