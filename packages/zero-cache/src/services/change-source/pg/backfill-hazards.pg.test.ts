import postgres from 'postgres';
import {expect} from 'vitest';
import {must} from '../../../../../shared/src/must.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {getConnectionURI, test} from '../../../test/db.ts';
import {INT4, TEXT} from '../../../types/pg-types.ts';
import {postgresTypeConfig} from '../../../types/pg.ts';
import {
  batch,
  lc,
  messages,
  relation,
  replica,
} from '../../replicator/backfill-repro-test-util.ts';
import {readBackfillRequests} from '../../replicator/schema/backfilling.ts';
import {
  getKeyCollations,
  isResumableKey,
  orderByRowKey,
  resumeWhere,
} from './backfill-resume.ts';

test('repro: resuming after a key moves behind the mark loses an omitted TOAST value', async ({
  testDBs,
}) => {
  await using db = await testDBs.create('backfill_key_move_repro');
  const r = replica('backup', [1, 2, 3, 4, 5, 6]);
  try {
    await db.unsafe(`CREATE TABLE items(id INT PRIMARY KEY, body TEXT);
      ALTER TABLE items ALTER COLUMN body SET STORAGE EXTERNAL;
      INSERT INTO items SELECT i, repeat('body ' || i, 2000) FROM generate_series(1, 6) i;`);
    const ordering = orderByRowKey(['id']);
    const prefix = await db.unsafe<{id: number; body: string}[]>(
      `SELECT * FROM items ORDER BY ${ordering} LIMIT 2`,
    );
    expect(prefix.map(row => row.id)).toEqual([1, 2]);
    r.transaction('02.01', {
      ...batch([]),
      rowValues: prefix.map(({id, body}) => [id, body]),
    });
    // A durable mark survives the crash. streamBackfill does not wire these
    // resume helpers in yet: this explicitly models that proposed behavior.
    const mark = [String(must(prefix.at(-1)).id)];
    await db`UPDATE items SET id = 0 WHERE id = 5`;
    // The unchanged external body is absent from the live update payload.
    r.transaction(
      '04',
      messages.update('items', {id: 0, label: 'row 5'}, {id: 5}),
    );
    const suffix = await db.unsafe<{id: number; body: string}[]>(
      `SELECT * FROM items WHERE ${resumeWhere(['id'], [{typeOID: INT4}], mark)} ORDER BY ${ordering}`,
    );
    expect(suffix.map(r => r.id)).toEqual([3, 4, 6]);
    r.transaction(
      '05',
      {...batch([], '05'), rowValues: suffix.map(({id, body}) => [id, body])},
      {tag: 'backfill-completed', relation, columns: ['body'], watermark: '05'},
    );
    expect(readBackfillRequests(r.db)).toEqual([]);
    expect(r.db.prepare('SELECT body FROM items WHERE id = 0').get()).toEqual({
      body: null,
    });
    expect((await db`SELECT body FROM items WHERE id = 0`)[0].body).toBe(
      'body 5'.repeat(2000),
    );
    expect(
      r.db
        .prepare('SELECT count(*) AS n FROM items WHERE body IS NOT NULL')
        .get(),
    ).toEqual({n: 5});
  } finally {
    r.db.close();
  }
});

test('repro: WIN1252 COLLATE C disagrees with SQLite, despite a resumable text key', async ({
  testDBs,
}) => {
  const name = 'backfill_win1252_repro';
  const admin = testDBs.sql;
  await admin`CREATE DATABASE ${admin(name)} TEMPLATE template0 ENCODING 'WIN1252' LC_COLLATE 'C' LC_CTYPE 'C'`;
  const uri = new URL(getConnectionURI(admin));
  uri.pathname = `/${name}`;
  const db = postgres(uri.toString(), {
    ...postgresTypeConfig(),
    connection: {client_encoding: 'UTF8'},
  });
  const lite = new Database(lc, ':memory:');
  try {
    expect((await db`SHOW server_encoding`)[0].server_encoding).toBe('WIN1252');
    expect((await db`SHOW client_encoding`)[0].client_encoding).toBe('UTF8');
    await db`CREATE TABLE items(k TEXT COLLATE "C" PRIMARY KEY)`;
    await db`INSERT INTO items VALUES ('a'), ('€'), ('é')`;
    const [{oid}] = await db<
      {oid: number}[]
    >`SELECT 'items'::regclass::oid AS oid`;
    const collations = await getKeyCollations(db, oid, ['k']);
    expect(
      isResumableKey([
        {typeOID: TEXT, collationIsDeterministic: collations.get('k')},
      ]),
    ).toBe(true);
    const rows = await db<
      {k: string; bytes: string}[]
    >`SELECT k, encode(convert_to(k, 'WIN1252'), 'hex') AS bytes FROM items ORDER BY k COLLATE "C"`;
    expect(rows.map(r => [r.k, r.bytes])).toEqual([
      ['a', '61'],
      ['€', '80'],
      ['é', 'e9'],
    ]);
    lite.exec('CREATE TABLE items(k TEXT PRIMARY KEY)');
    for (const {k} of rows) {
      lite.prepare('INSERT INTO items VALUES (?)').run(k);
    }
    expect(lite.prepare('SELECT k FROM items ORDER BY k').all()).toEqual([
      {k: 'a'},
      {k: 'é'},
      {k: '€'},
    ]);
    // A local <= mark check incorrectly counts é as already backfilled.
    expect(
      lite.prepare('SELECT k FROM items WHERE k <= ? ORDER BY k').all('€'),
    ).toEqual([{k: 'a'}, {k: 'é'}, {k: '€'}]);
    expect(
      await db`SELECT k FROM items WHERE k > '€' COLLATE "C" ORDER BY k COLLATE "C"`,
    ).toEqual([{k: 'é'}]);
    // The current helper keeps comparisons in Postgres, which is safe even
    // here. This guards against introducing a local byte-order comparison.
    expect(
      await db.unsafe(
        `SELECT k FROM items WHERE ${resumeWhere(['k'], [{typeOID: TEXT}], ['€'])} ORDER BY ${orderByRowKey(['k'])}`,
      ),
    ).toEqual([{k: 'é'}]);
  } finally {
    lite.close();
    await db.end();
    await admin`DROP DATABASE ${admin(name)}`;
  }
});
