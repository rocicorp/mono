// Hydration benchmarks against the Pagila dataset.
//
// Pagila is several times larger than Chinook (16k rentals, 5k film_actor,
// 4.5k inventory, ~14k payments) and exercises shapes Chinook doesn't —
// many-to-many junction tables (film_actor, film_category), partitioned
// payment tables, and a 4-level geographic chain (customer → address →
// city → country).

import {runBenchmarks} from '../../zql-integration-tests/src/helpers/runner.ts';
import {getPagila} from '../../zql-integration-tests/src/pagila/get-deps.ts';
import {schema} from '../../zql-integration-tests/src/pagila/schema.ts';

const pgContent = await getPagila();

await runBenchmarks(
  {
    suiteName: 'pagila_bench_hydrate',
    type: 'hydration',
    pgContent,
    zqlSchema: schema,
  },
  [
    // ---- Single-table fetches ------------------------------------------------
    {
      name: '(table scan, large) select * from rental',
      createQuery: q => q.rental,
    },
    {
      name: '(pk lookup) select * from film where id = 100',
      createQuery: q => q.film.where('id', 100),
    },
    {
      name: '(secondary index lookup) rental.where(customerId = 100)',
      createQuery: q => q.rental.where('customerId', 100),
    },
    {
      name: '(non-indexed scan + filter) film.where(rating="PG-13")',
      createQuery: q => q.film.where('rating', 'PG-13'),
    },
    {
      name: '(LIKE filter) film.where(title LIKE "%LOVE%")',
      createQuery: q => q.film.where('title', 'LIKE', '%LOVE%'),
    },

    // ---- Many-to-many junctions ----------------------------------------------
    // film -> actors goes film -> film_actor -> actor (junction with composite
    // PK actor_id, film_id). 1k films × ~5 actors each.
    {
      name: 'film with actors (m2m, limit 50)',
      createQuery: q => q.film.related('actors').limit(50),
    },
    {
      name: 'actor with films (m2m, limit 50)',
      createQuery: q => q.actor.related('films').limit(50),
    },
    {
      name: 'film with actors AND categories (limit 50)',
      createQuery: q =>
        q.film.related('actors').related('categories').limit(50),
    },

    // ---- Multi-hop chains ----------------------------------------------------
    // rental → inventory → film → language is the canonical 4-deep chain.
    {
      name: 'rental → inventory → film → language (limit 200)',
      createQuery: q =>
        q.rental
          .related('inventory', i =>
            i.related('film', f => f.related('language')),
          )
          .limit(200),
    },
    // 4-level geographic chain.
    {
      name: 'country → cities → addresses (full)',
      createQuery: q =>
        q.country.related('cities', c => c.related('addresses')),
    },

    // ---- Wide fan-out --------------------------------------------------------
    {
      name: 'address with customers, staff, stores (limit 100)',
      createQuery: q =>
        q.address
          .related('customers')
          .related('staff')
          .related('stores')
          .limit(100),
    },
    {
      name: 'customer with rentals and payments (limit 50)',
      createQuery: q =>
        q.customer.related('rentals').related('payments').limit(50),
    },

    // ---- whereExists shapes --------------------------------------------------
    // 4-level whereExists chain mirrors the planner-exec test.
    {
      name: 'customer.exists(address.city.country = US) limit 20',
      createQuery: q =>
        q.customer
          .whereExists('address', a =>
            a.whereExists('city', c =>
              c.whereExists('country', co =>
                co.where('country', 'United States'),
              ),
            ),
          )
          .limit(20),
    },
    // junction-table whereExists.
    {
      name: 'film.exists(actors.lastName = "GUINESS")',
      createQuery: q =>
        q.film.whereExists('actors', a => a.where('lastName', 'GUINESS')),
    },

    // ---- OR shapes (mixes UnionFanOut/UnionFanIn) ---------------------------
    {
      name: 'film.where(rating=PG OR rating=G) related actors limit 50',
      createQuery: q =>
        q.film
          .where(({or, cmp}) => or(cmp('rating', 'PG'), cmp('rating', 'G')))
          .related('actors')
          .limit(50),
    },
  ],
);
