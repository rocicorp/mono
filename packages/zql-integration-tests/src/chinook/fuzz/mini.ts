/**
 * A small, hand-authored fixture on the **chinook schema** for the coverage-driven
 * fuzzer (ported from rusty-ivm `rindle-testfix/src/mini.rs`). ~40 FK-consistent rows,
 * deliberately shaped to exercise the transitions the generator relies on:
 *
 * - an artist with **no albums** (Gamma / id 3), a genre with **no tracks**
 *   (Empty / id 4), an **empty playlist** (P-two / id 2), a customer with **no
 *   invoices** (id 3) — for EXISTS / NOT EXISTS flips and empty children;
 * - albums with **exactly 2–3 tracks** (10→3, 11→2, 20→3) — for limit boundaries;
 * - **NULL**s (some `composer`, customer 3's `country`, the boss's `reportsTo`) —
 *   for IS NULL / null ordering;
 * - a **self-referential** employee chain (1←2←3) and a **junction**
 *   (`playlist_track`).
 *
 * The dataset is the single source of truth: {@link miniPgContent} derives both the
 * DDL and the seed INSERTs from it + the zql `schema`, so the PG fixture, the SQLite
 * replica, and the in-memory sources all hold identical data, and the data can never
 * drift from the schema.
 *
 * Numbers map to PG `DOUBLE PRECISION` and text to `TEXT` (the chinook schema models
 * every column as `number()` or `string()`); date columns (`invoiceDate`, …), which
 * the schema also models as `number()`, are seeded as plain integers so there is no
 * timestamp conversion in the mini path (the full-chinook nightly exercises that).
 */

import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../../zero-types/src/schema.ts';
import {schema} from '../schema.ts';

/** The curated mini dataset, keyed by **client** table name, in **client** columns. */
export const miniData: Record<string, Row[]> = {
  artist: [
    {id: 1, name: 'Alpha'},
    {id: 2, name: 'Beta'},
    {id: 3, name: 'Gamma'}, // no albums
  ],
  album: [
    {id: 10, title: 'A-one', artistId: 1},
    {id: 11, title: 'A-two', artistId: 1},
    {id: 20, title: 'B-one', artistId: 2},
  ],
  genre: [
    {id: 1, name: 'Rock'},
    {id: 2, name: 'Jazz'},
    {id: 3, name: 'Metal'},
    {id: 4, name: 'Empty'}, // no tracks
  ],
  mediaType: [
    {id: 1, name: 'MP3'},
    {id: 2, name: 'AAC'},
  ],
  track: [
    // (id, name, albumId, mediaTypeId, genreId, composer, ms, bytes, price)
    trk(100, 't-a', 10, 1, 1, 'Smith', 200_000, 2_000_000, 0.99),
    trk(101, 't-b', 10, 1, 1, null, 100_000, 1_000_000, 0.99),
    trk(102, 't-c', 10, 2, 2, 'Jones', 300_000, 3_000_000, 1.29),
    trk(103, 't-d', 11, 1, 3, 'Smith', 150_000, 1_500_000, 0.99),
    trk(104, 't-e', 11, 2, 1, null, 250_000, 2_500_000, 0.99),
    trk(105, 't-f', 20, 1, 2, 'Lee', 50_000, 500_000, 0.99),
    trk(106, 't-g', 20, 1, 3, 'Smith', 400_000, 4_000_000, 1.29),
    trk(107, 't-h', 20, 2, 1, null, 350_000, 3_500_000, 0.99),
  ],
  playlist: [
    {id: 1, name: 'P-one'},
    {id: 2, name: 'P-two'}, // empty
  ],
  playlistTrack: [
    {playlistId: 1, trackId: 100},
    {playlistId: 1, trackId: 101},
    {playlistId: 1, trackId: 102},
  ],
  employee: [
    // self-referential chain 1 (boss) ← 2 ← 3
    emp(1, 'Adams', 'Andrew', 'GM', null, 'NYC', 'USA'),
    emp(2, 'Mills', 'Margaret', 'Sales', 1, 'LA', 'USA'),
    emp(3, 'Park', 'Peter', 'Support', 2, 'SF', 'USA'),
  ],
  customer: [
    cust(1, 'Ann', 'Aaronson', 'USA', 2),
    cust(2, 'Bo', 'Becker', 'UK', 2),
    cust(3, 'Cy', 'Carter', null, null), // no invoices, null country/supportRep
  ],
  invoice: [
    {id: 1000, customerId: 1, invoiceDate: 20_200_101, total: 10},
    {id: 1001, customerId: 1, invoiceDate: 20_200_201, total: 5},
    {id: 1002, customerId: 2, invoiceDate: 20_200_301, total: 7},
  ],
  invoiceLine: [
    {id: 5000, invoiceId: 1000, trackId: 100, unitPrice: 0.99, quantity: 1},
    {id: 5001, invoiceId: 1000, trackId: 101, unitPrice: 0.99, quantity: 2},
    {id: 5002, invoiceId: 1002, trackId: 105, unitPrice: 0.99, quantity: 1},
  ],
};

function trk(
  id: number,
  name: string,
  albumId: number,
  mediaTypeId: number,
  genreId: number,
  composer: string | null,
  milliseconds: number,
  bytes: number,
  unitPrice: number,
): Row {
  return {
    id,
    name,
    albumId,
    mediaTypeId,
    genreId,
    composer,
    milliseconds,
    bytes,
    unitPrice,
  };
}

function emp(
  id: number,
  lastName: string,
  firstName: string,
  title: string,
  reportsTo: number | null,
  city: string,
  country: string,
): Row {
  return {id, lastName, firstName, title, reportsTo, city, country};
}

function cust(
  id: number,
  firstName: string,
  lastName: string,
  country: string | null,
  supportRepId: number | null,
): Row {
  return {
    id,
    firstName,
    lastName,
    country,
    supportRepId,
    email: `${firstName.toLowerCase()}@example.com`,
  };
}

// ── PG fixture generation (DDL + seed INSERTs derived from the schema + data) ─────────

function pgType(type: string): string {
  switch (type) {
    case 'number':
      return 'DOUBLE PRECISION';
    case 'boolean':
      return 'BOOLEAN';
    default:
      return 'TEXT';
  }
}

function serverTable(s: Schema, table: string): string {
  return s.tables[table].serverName ?? table;
}

function serverColumn(s: Schema, table: string, column: string): string {
  return s.tables[table].columns[column].serverName ?? column;
}

function sqlValue(v: unknown): string {
  if (v === null || v === undefined) {
    return 'NULL';
  }
  if (typeof v === 'number') {
    return String(v);
  }
  if (typeof v === 'boolean') {
    return v ? 'TRUE' : 'FALSE';
  }
  return `'${String(v).replaceAll("'", "''")}'`;
}

/** `CREATE TABLE` for one client table, in server names/types, with PK + nullability. */
function ddlFor(s: Schema, table: string): string {
  const tableSchema = s.tables[table];
  const cols = Object.entries(tableSchema.columns).map(([name, col]) => {
    const nullable = col.optional ? '' : ' NOT NULL';
    return `  ${serverColumn(s, table, name)} ${pgType(col.type)}${nullable}`;
  });
  const pk = tableSchema.primaryKey.map(c => serverColumn(s, table, c));
  cols.push(`  PRIMARY KEY (${pk.join(', ')})`);
  return `CREATE TABLE ${serverTable(s, table)} (\n${cols.join(',\n')}\n);`;
}

/** `INSERT` statements for one client table (all schema columns, absent ⇒ NULL). */
function insertsFor(s: Schema, table: string): string {
  const rows = miniData[table];
  if (!rows || rows.length === 0) {
    return '';
  }
  const columns = Object.keys(s.tables[table].columns);
  const serverCols = columns.map(c => serverColumn(s, table, c));
  const valueRows = rows.map(
    row => `  (${columns.map(c => sqlValue(row[c])).join(', ')})`,
  );
  return `INSERT INTO ${serverTable(s, table)} (${serverCols.join(
    ', ',
  )}) VALUES\n${valueRows.join(',\n')};`;
}

/**
 * The full `pgContent` for the mini fixture: every table's DDL followed by its seed
 * INSERTs, in dependency-free order (FKs are not declared, matching `get-deps.ts`).
 */
export function miniPgContent(s: Schema = schema): string {
  const tables = Object.keys(s.tables);
  const ddl = tables.map(t => ddlFor(s, t)).join('\n\n');
  const inserts = tables
    .map(t => insertsFor(s, t))
    .filter(Boolean)
    .join('\n\n');
  return `${ddl}\n\n${inserts}\n`;
}
