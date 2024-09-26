import {LogContext} from '@rocicorp/logger';
import type {QueryDefs} from 'zero-client';
import {Database} from 'zqlite/src/db.js';
import {ZQLiteZero} from 'zqlite/src/zqlite-zero.js';

export function newSqliteZero<QD extends QueryDefs>(
  lc: LogContext,
  schemas: QD,
): ZQLiteZero<QD> {
  {
    const db = new Database(lc, ':memory:');
    const z = new ZQLiteZero({
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
