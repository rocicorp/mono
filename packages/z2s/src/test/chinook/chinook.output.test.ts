import {expect, test} from 'vitest';
import {
  StaticQuery,
  staticQuery,
} from '../../../../zql/src/query/static-query.ts';
import {schema} from './schema.ts';
import type {Query} from '../../../../zql/src/query/query.ts';
import {compile} from '../../compiler.ts';
import {formatPg} from '../../sql.ts';
type Schema = typeof schema;

test('limited junction edge', () => {
  const q = staticQuery(schema, 'playlist').related('tracks', q => q.limit(10));
  expect(getSQL(q)).toMatchInlineSnapshot(`
    "SELECT (
            SELECT COALESCE(array_agg(row_to_json("inner_tracks")) , ARRAY[]::json[]) FROM (SELECT "table_1".* FROM "playlist_track" JOIN "track" as "table_1" ON "playlist_track"."track_id" = "table_1"."track_id" WHERE ("playlist"."playlist_id" = "playlist_track"."playlist_id")  ORDER BY "playlist_id" ASC, "track_id" ASC LIMIT $1 ) "inner_tracks"
          ) as "tracks","playlist_id","name" FROM "playlist"   ORDER BY "playlist_id" ASC"
  `);
});

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function getSQL(q: Query<any, any, any>) {
  return formatPg(compile(ast(q), schema.tables, format(q))).text;
}

function ast(q: Query<Schema, keyof Schema['tables']>) {
  return (q as StaticQuery<Schema, keyof Schema['tables']>).ast;
}

function format(q: Query<Schema, keyof Schema['tables']>) {
  return (q as StaticQuery<Schema, keyof Schema['tables']>).format;
}
