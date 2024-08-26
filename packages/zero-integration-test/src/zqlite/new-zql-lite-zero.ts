import {QueryDefs} from 'zero-client';
import {ZqlLiteZero} from 'zqlite/src/zqlite-zero.js';
import Database from 'better-sqlite3';

export function newSqliteZero<QD extends QueryDefs>(
  schemas: QD,
): ZqlLiteZero<QD> {
  {
    const db = new Database(':memory:');
    const z = new ZqlLiteZero({
      schemas,
      db,
    });
    if (!db) {
      throw new Error('db is undefined');
    }
    db.prepare('CREATE TABLE artist (id TEXT PRIMARY KEY, name TEXT)').run();
    db.prepare(
      'CREATE TABLE album (id TEXT PRIMARY KEY, title TEXT, artistId TEXT)',
    ).run();
    db.prepare(
      'CREATE TABLE track (id TEXT PRIMARY KEY, length NUMBER, title TEXT, albumId TEXT)',
    ).run();
    db.prepare(
      'CREATE TABLE trackArtist (id TEXT PRIMARY KEY, artistId TEXT, trackId TEXT)',
    ).run();
    return z;
  }
}
