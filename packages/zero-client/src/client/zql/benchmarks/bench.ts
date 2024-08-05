import * as SQLite from 'wa-sqlite';
import SQLiteAsyncESMFactory from 'wa-sqlite/dist/wa-sqlite-async.mjs';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import {IDBBatchAtomicVFS} from 'wa-sqlite/src/examples/IDBBatchAtomicVFS.js';
import type {EntityQuery} from '../../../../../zql/src/zql/query/entity-query.js';
import type {Statement} from '../../../../../zql/src/zql/query/statement.js';
import {
  Album,
  Artist,
  bulkRemove,
  bulkSet,
  newZero,
  Track,
  TrackArtist,
} from '../integration-test-util.js';

const wasmModule = await SQLiteAsyncESMFactory();
const sqlite3 = SQLite.Factory(wasmModule);
sqlite3.vfs_register(
  new IDBBatchAtomicVFS('idb-batch-atomic', {durability: 'relaxed'}),
);

export type BulkItems = {
  tracks?: Track[] | undefined;
  albums?: Album[] | undefined;
  artists?: Artist[] | undefined;
  trackArtists?: TrackArtist[] | undefined;
};

export type Queries = {
  trackQuery: EntityQuery<{track: Track}>;
  albumQuery: EntityQuery<{album: Album}>;
  artistQuery: EntityQuery<{artist: Artist}>;
  trackArtistQuery: EntityQuery<{trackArtist: TrackArtist}>;
};

export type Mutator = (
  upsertMany: (data: BulkItems) => Promise<void>,
  deleteMany: (data: BulkItems) => Promise<void>,
) => Promise<void>;

export async function benchZQL(
  preRun: Mutator | undefined,
  queries: readonly (readonly [
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    query: (queries: Queries) => EntityQuery<any, any, any>,
    times: number,
  ])[],
  mutations: readonly (readonly [mutation: Mutator, times: number])[],
): Promise<number> {
  const z = newZero();

  async function upsertMany(data: BulkItems) {
    await bulkSet(z, data);
  }
  async function deleteMany(data: BulkItems) {
    await bulkRemove(z, data);
  }

  const queryRunners = queries.map(zql => {
    if (zql === undefined) {
      return [() => Promise.resolve([]), 0] as const;
    }
    let stmt: Statement<unknown[]> | undefined = undefined;
    return [
      () => {
        if (stmt === undefined) {
          // statement prep eagerly runs the query so
          // to be fair we do this work inside the benchmark.
          // hence the `stmt === undefined` check and not eagerly
          // preparing.
          stmt = zql[0]({
            trackQuery: z.query.track,
            albumQuery: z.query.album,
            artistQuery: z.query.artist,
            trackArtistQuery: z.query.trackArtist,
          }).prepare();
        }

        return stmt.exec();
      },
      zql[1],
    ] as const;
  });

  const mutationRunners = mutations.map(
    ([mutator, times]) =>
      [() => mutator(upsertMany, deleteMany), times] as const,
  );

  if (preRun !== undefined) {
    await preRun(upsertMany, deleteMany);
  }
  const ret = await runBenchmark(
    queryRunners,
    mutationRunners,
    () => Promise.resolve(),
    () => Promise.resolve(),
  );

  await z.close();
  // await removeAllIDbDbs();
  return ret;
}

// No indices as well. Is it fair? Idk. We don't use indices in ZQL rn.
// We should bench both.
export async function benchSQLite(
  preRun: Mutator | undefined,
  queries: readonly (readonly [string, number])[],
  mutations: readonly (readonly [mutation: Mutator, times: number])[],
): Promise<number> {
  const db = await sqlite3.open_v2('test-db');

  await sqlite3.exec(
    db,
    /*sql*/ `
    DROP TABLE IF EXISTS track;
    DROP TABLE IF EXISTS album;
    DROP TABLE IF EXISTS artist;
    DROP TABLE IF EXISTS trackArtist;
    VACUUM;
  `,
  );

  // SQLite gets an edge since it has primary key indices it can use.
  // ZQL has them but doesn't do anything smary with them rn.
  await sqlite3.exec(
    db,
    /*sql*/ `
    CREATE TABLE track (
      id TEXT PRIMARY KEY,
      title TEXT,
      length INTEGER,
      albumId TEXT
    );
    CREATE TABLE album (
      id TEXT PRIMARY KEY,
      title TEXT,
      artistId TEXT
    );
    CREATE TABLE artist (
      id TEXT PRIMARY KEY,
      name TEXT
    );
    CREATE TABLE trackArtist (
      id TEXT PRIMARY KEY,
      trackId TEXT,
      artistId TEXT
    );
  `,
  );

  const insertTrack = await prepare(
    db,
    /*sql*/ `INSERT OR REPLACE INTO track (id, title, length, albumId) VALUES (?, ?, ?, ?)`,
  );
  const insertAlbum = await prepare(
    db,
    /*sql*/ `INSERT OR REPLACE INTO album (id, title, artistId) VALUES (?, ?, ?)`,
  );
  const insertArtist = await prepare(
    db,
    /*sql*/ `INSERT OR REPLACE INTO artist (id, name) VALUES (?, ?)`,
  );
  const insertTrackArtist = await prepare(
    db,
    /*sql*/ `INSERT OR REPLACE INTO trackArtist (id, trackId, artistId) VALUES (?, ?, ?)`,
  );
  const deleteTracks = await prepare(
    db,
    /*sql*/ `DELETE FROM track WHERE id = ?`,
  );
  const begin = await prepare(db, 'BEGIN');
  const commit = await prepare(db, 'COMMIT');
  const rollback = await prepare(db, 'ROLLBACK');

  async function upsertMany({
    tracks,
    albums,
    artists,
    trackArtists,
  }: BulkItems) {
    // SQLite _must_ be called serially. It doesn't support parallel awaits /
    // that'll deadlock it in the browser.
    await sqlite3.step(begin);
    try {
      for (const track of tracks ?? []) {
        sqlite3.bind_text(insertTrack, 1, track.id);
        sqlite3.bind_text(insertTrack, 2, track.title);
        sqlite3.bind_int(insertTrack, 3, track.length);
        sqlite3.bind_text(insertTrack, 4, track.albumId);
        await sqlite3.step(insertTrack);
        await sqlite3.reset(insertTrack);
      }

      for (const trackArtist of trackArtists ?? []) {
        sqlite3.bind_text(insertTrackArtist, 1, trackArtist.id);
        sqlite3.bind_text(insertTrackArtist, 2, trackArtist.trackId);
        sqlite3.bind_text(insertTrackArtist, 3, trackArtist.artistId);
        await sqlite3.step(insertTrackArtist);
        await sqlite3.reset(insertTrackArtist);
      }

      for (const album of albums ?? []) {
        sqlite3.bind_text(insertAlbum, 1, album.id);
        sqlite3.bind_text(insertAlbum, 2, album.title);
        sqlite3.bind_text(insertAlbum, 3, album.artistId);
        await sqlite3.step(insertAlbum);
        await sqlite3.reset(insertAlbum);
      }

      for (const artist of artists ?? []) {
        sqlite3.bind_text(insertArtist, 1, artist.id);
        sqlite3.bind_text(insertArtist, 2, artist.name);
        await sqlite3.step(insertArtist);
        await sqlite3.reset(insertArtist);
      }
      await sqlite3.step(commit);
    } catch (e) {
      await sqlite3.step(rollback);
    } finally {
      await sqlite3.reset(begin);
      await sqlite3.reset(commit);
      await sqlite3.reset(rollback);
    }
  }

  async function deleteMany({
    tracks,
    albums,
    artists,
    trackArtists,
  }: BulkItems) {
    try {
      await sqlite3.step(begin);
      for (const track of tracks ?? []) {
        sqlite3.bind_text(deleteTracks, 1, track.id);
        await sqlite3.step(deleteTracks);
        await sqlite3.reset(deleteTracks);
      }

      for (const artist of artists ?? []) {
        sqlite3.bind_text(deleteTracks, 1, artist.id);
        await sqlite3.step(deleteTracks);
        await sqlite3.reset(deleteTracks);
      }

      for (const album of albums ?? []) {
        sqlite3.bind_text(deleteTracks, 1, album.id);
        await sqlite3.step(deleteTracks);
        await sqlite3.reset(deleteTracks);
      }

      for (const trackArtist of trackArtists ?? []) {
        sqlite3.bind_text(deleteTracks, 1, trackArtist.id);
        await sqlite3.step(deleteTracks);
        await sqlite3.reset(deleteTracks);
      }

      await sqlite3.step(commit);
    } catch (e) {
      await sqlite3.step(rollback);
    } finally {
      await sqlite3.reset(begin);
      await sqlite3.reset(commit);
      await sqlite3.reset(rollback);
    }
  }

  async function getRows(stmt: number) {
    const rows = [];
    // const cols = sqlite3.column_names(stmt);
    while ((await sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {
      rows.push(sqlite3.row(stmt));
    }

    await sqlite3.reset(stmt);
    return rows;
  }

  const preparedQueries: (readonly [
    query: () => Promise<SQLiteCompatibleType[][]>,
    times: number,
  ])[] = [];
  for (const sql of queries) {
    const stmt = await prepare(db, sql[0]);
    preparedQueries.push([() => getRows(stmt), sql[1]] as const);
  }

  const preparedMutations = mutations.map(
    ([mutator, times]) =>
      [() => mutator(upsertMany, deleteMany), times] as const,
  );

  if (preRun !== undefined) {
    await preRun(upsertMany, deleteMany);
  }
  return runBenchmark(
    preparedQueries,
    preparedMutations,
    async () => {
      await sqlite3.step(begin);
      await sqlite3.reset(begin);
    },
    async () => {
      await sqlite3.step(commit);
      await sqlite3.reset(commit);
    },
  );
}

async function runBenchmark(
  queries: (readonly [
    query: () => Promise<readonly unknown[]>,
    times: number,
  ])[],
  mutations: (readonly [mutation: () => Promise<void>, times: number])[],
  txStart: () => Promise<void>,
  txEnd: () => Promise<void>,
) {
  const start = performance.now();
  async function runQueries() {
    await txStart();
    for (const [query, times] of queries) {
      for (let i = 0; i < times; i++) {
        await query();
      }
    }
    await txEnd();
  }

  for (const [mutation, times] of mutations) {
    for (let i = 0; i < times; i++) {
      await mutation();
      await runQueries();
    }
  }

  // if there were no mutations then we just want to query against initial data
  if (mutations.length === 0) {
    await runQueries();
  }

  return performance.now() - start;
}

async function prepare(db: number, sql: string) {
  return (await sqlite3.statements(db, sql)[Symbol.asyncIterator]().next())
    .value as number;
}
