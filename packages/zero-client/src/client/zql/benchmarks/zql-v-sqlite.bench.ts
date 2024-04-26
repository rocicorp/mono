import {
  createRandomAlbums,
  createRandomArtists,
  createRandomTracks,
  // linkTracksToArtists,
} from './setup.js';
import {Queries, benchSQLite, benchZQL, type BulkItems} from './bench.js';

/*
vitest supports benchmarking but it is currently
broken in the browser runner:
  - https://vitest.dev/guide/features.html#benchmarking
  - https://github.com/vitest-dev/vitest/pull/1029
  - https://github.com/vitest-dev/vitest/issues/5041

Doing this as a test w/ console.log for now
*/

const suite = async () => {
  function upsertNTracksMutation(n: number) {
    const tracks = createRandomTracks(
      n,
      createRandomAlbums(1, createRandomArtists(1)),
      true,
    );
    return async (upsertMany: (data: BulkItems) => Promise<void>) => {
      await upsertMany({tracks});
    };
  }
  function upserNTracksTracksWithSomeTitles(n: number, titles: string[]) {
    const tracks = createRandomTracks(
      n,
      createRandomAlbums(1, createRandomArtists(1)),
      true,
    );
    const stride = n / titles.length;
    for (let i = 0; i < n; i += stride) {
      tracks[i].title = titles[i / stride];
    }
    return async (upsertMany: (data: BulkItems) => Promise<void>) => {
      await upsertMany({tracks});
    };
  }
  const cases = [
    {
      name: '1k serial inserts in individual transactions',
      mutations: [[upsertNTracksMutation(1), 1_000]] as const,
      queries: [] as const,
    },
    {
      name: 'insert followed by range select, 1k times serially',
      mutations: [[upsertNTracksMutation(1), 1_000]] as const,
      queries: [
        {
          zql: ({trackQuery}: Queries) => trackQuery.select().limit(100),
          sql: /*sql*/ `SELECT * FROM track LIMIT 100`,
          repeat: 1,
        },
      ] as const,
    },
    {
      // ZQL is dumb and does a scan even for a point query against a primary key.
      // Even though all future invocations of the query are instant, SQLite still
      // can do 1k reapeats of the same query in the time it takes ZQL to do 1.
      name: 'Single point query (id = x) against a table of 10,000 items',
      setup: async (upsertMany: (data: BulkItems) => Promise<void>) => {
        await upsertNTracksMutation(10_000)(upsertMany);
      },
      mutations: [],
      queries: [
        {
          // TODO: ZQL is dumb and will still filter the entire collection even though
          // we got an exact match on primary key
          zql: ({trackQuery}: Queries) =>
            trackQuery.select().where('id', '=', '1'),
          sql: /*sql*/ `SELECT * FROM track WHERE id = '500'`,
          repeat: 1,
        },
      ] as const,
    },
    {
      // ZQL is dumb and does a scan even for a point query against a primary key.
      // Even though all future invocations of the query are instant, SQLite still
      // can do 1k reapeats of the same query in the time it takes ZQL to do 1.
      name: 'Repeat the exact same point query (id = x) 1k times against a table of 10,000 items',
      setup: async (upsertMany: (data: BulkItems) => Promise<void>) => {
        await upsertNTracksMutation(10_000)(upsertMany);
      },
      mutations: [],
      queries: [
        {
          zql: ({trackQuery}: Queries) =>
            trackQuery.select().where('id', '=', '500'),
          sql: /*sql*/ `SELECT * FROM track WHERE id = '500'`,
          repeat: 1_000,
        },
      ] as const,
    },
    {
      name: '100 unique point queries (id = x) against a table of 10,000 items',
      setup: async (upsertMany: (data: BulkItems) => Promise<void>) => {
        await upsertNTracksMutation(10_000)(upsertMany);
      },
      mutations: [],
      queries: Array.from({length: 100}, () => {
        const id = Math.floor(Math.random() * 10_000).toString();
        return {
          zql: ({trackQuery}: Queries) =>
            trackQuery.select().where('id', '=', id),
          sql: /*sql*/ `SELECT * FROM track WHERE id = '${id}'`,
          repeat: 1,
        };
      }),
    },
    // TODO: table scan against primary key
    // TODO: table scan against indexed column
    // TODO: what does a ZQL scan look like when the `experimentalWatch` step has alreay completed?
    // TODO: what does the TPS slowdown look like as we add more queries to maintain?
    {
      name: 'Table scan against 100,000 items (no index)',
      setup: async (upsertMany: (data: BulkItems) => Promise<void>) => {
        await upsertNTracksMutation(100_000)(upsertMany);
      },
      mutations: [],
      queries: [
        {
          zql: ({trackQuery}: Queries) =>
            trackQuery.select().where('title', '=', 'foo'),
          sql: /*sql*/ `SELECT * FROM track WHERE title = 'foo'`,
          repeat: 1,
        },
      ] as const,
    },
    {
      name: 'Repeat the exact same table scan against 100,000 items (no index) 100 times',
      setup: async (upsertMany: (data: BulkItems) => Promise<void>) => {
        await upsertNTracksMutation(100_000)(upsertMany);
      },
      mutations: [],
      queries: [
        {
          zql: ({trackQuery}: Queries) =>
            trackQuery.select().where('title', '=', 'foo'),
          sql: /*sql*/ `SELECT * FROM track WHERE title = 'foo'`,
          repeat: 100,
        },
      ] as const,
    },
    {
      name: 'Table scan against 10,000 items (no index)',
      setup: async (upsertMany: (data: BulkItems) => Promise<void>) => {
        await upsertNTracksMutation(10_000)(upsertMany);
      },
      mutations: [],
      queries: [
        {
          zql: ({trackQuery}: Queries) =>
            trackQuery.select().where('title', '=', 'foo'),
          sql: /*sql*/ `SELECT * FROM track WHERE title = 'foo'`,
          repeat: 1,
        },
      ] as const,
    },
    {
      name: 'table scan 10,000 items for 1k matching items (no index), limit 100',
      setup: async (upsertMany: (data: BulkItems) => Promise<void>) => {
        await upserNTracksTracksWithSomeTitles(
          10_000,
          Array.from({length: 1_000}, (_, i) => `title${i}`),
        )(upsertMany);
      },
      mutations: [],
      queries: [
        {
          zql: ({trackQuery}: Queries) =>
            trackQuery.select().where('title', 'LIKE', 'title%').limit(100),
          sql: /*sql*/ `SELECT * FROM track WHERE title LIKE 'title%' LIMIT 100`,
          repeat: 1,
        },
      ] as const,
      // TODO: add a validator function to ensure correct data was returned.
      // I.e., it'd suck to have bugs in our benchmarks and not be comparing what we think we are
    },
    {
      name: 'Maintain 100 range queries (limit 100) against a table of 1,000 items over 500 mutations',
      mutations: [
        [upsertNTracksMutation(1_000), 1],
        [upsertNTracksMutation(1), 500],
      ] as const,
      queries: Array.from({length: 100}, () => ({
        // each of these does a full scan of the 10k because:
        // 1. we do not share between overlapping queries yet
        // 2. we do not respect hoisting and limiting if we're in source order
        zql: ({trackQuery}: Queries) => trackQuery.select().limit(100),
        sql: /*sql*/ `SELECT * FROM track LIMIT 100`,
        repeat: 1,
      })),
    },
    // TODO: benchmark more complex queries (e.g., join.integration.test type of things)
  ];

  for (const {name, setup, mutations, queries} of cases) {
    console.log(name);
    const green = '[92m';
    const red = '[91m';
    const term = '[0m';
    const zqlTime = await benchZQL(
      setup,
      queries.map(q => [q.zql, q.repeat] as const),
      mutations,
    );
    const sqliteTime = await benchSQLite(
      setup,
      queries.map(q => [q.sql, q.repeat] as const),
      mutations,
    );
    let zqlColor = green;
    let sqliteColor = red;
    if (zqlTime > sqliteTime) {
      zqlColor = red;
      sqliteColor = green;
    }
    console.log(
      `ZQL:\t${zqlColor}${zqlTime.toFixed(
        2,
      )}${term}ms\nSQLite:\t${sqliteColor}${sqliteTime.toFixed(2)}${term}ms`,
    );
    console.log('\n');
  }
};

await suite();
