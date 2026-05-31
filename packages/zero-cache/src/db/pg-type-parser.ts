import {assert} from '../../../shared/src/asserts.ts';
import {mapEntries} from '../../../shared/src/objects.ts';
import {JSON, JSONB} from '../types/pg-types.ts';
import type {PostgresDB} from '../types/pg.ts';

// Arbitrary array type to test if the PostgresDB client has fetched types.
const INT4_ARRAY_TYPE = 1007;

export type TypeParser = (val: string) => unknown;
export interface TypeParsers {
  getTypeParser(typeOID: number): TypeParser;
}

export type ParseOptions = {
  /**
   * Returns JSON and JSONB values as raw JSON strings (i.e. unparsed).
   * Note that JSON[] and JSONB[] values are returned as arrays of (parsed)
   * JSON values.
   */
  returnJsonAsString?: boolean;
};

const JSON_TYPE = String(JSON);
const JSONB_TYPE = String(JSONB);

// postgres.js has default type parsers with user-defined overrides
// configurable per-client (see `postgresTypeConfig` in types/pg.ts).
//
// From these, the postgres.js client will automatically derive parsers
// for array versions of these types, provided that the client was
// configured with `fetch_types: true` (which is the default).
//
// A replication session (with `database: replication`), however, does
// not support this type fetching, so it is done on a connection from
// a default client.
export async function getTypeParsers(
  db: PostgresDB,
  {returnJsonAsString}: ParseOptions = {},
): Promise<TypeParsers> {
  if (!db.options.parsers[INT4_ARRAY_TYPE]) {
    assert(db.options.fetch_types, `Supplied db must fetch_types`);

    // Execute a query to ensure that fetchArrayTypes() gets executed:
    // https://github.com/porsager/postgres/blob/089214e85c23c90cf142d47fb30bd03f42874984/src/connection.js#L536
    await db`SELECT 1`.simple();
    assert(
      db.options.parsers[INT4_ARRAY_TYPE],
      `array types not fetched ${Object.keys(db.options.parsers)}`,
    );
  }
  const parsers = mapEntries(db.options.parsers, (type, parse) => {
    if ((type === JSON_TYPE || type === JSONB_TYPE) && returnJsonAsString) {
      return [type, (x: string) => x];
    }
    // The postgres.js library tags parsers for array types with an `array: true` field.
    // https://github.com/porsager/postgres/blob/089214e85c23c90cf142d47fb30bd03f42874984/src/connection.js#L760
    const isArrayType = (parse as unknown as {array?: boolean}).array;

    // And then skips the first character when parsing the string,
    // e.g. an array parser will parse '{1,2,3}' from '1,2,3}'.
    // https://github.com/porsager/postgres/blob/089214e85c23c90cf142d47fb30bd03f42874984/src/connection.js#L496
    return [
      type,
      isArrayType ? (val: string) => parse(val.substring(1)) : parse,
    ];
  });
  return {
    // A type OID for which a parser is not explicitly defined
    // is parsed as a String.
    // https://github.com/porsager/postgres/blob/b0d8c8f363e006a74472d76f859da60c52a80368/src/connection.js#L494
    //
    // This is also consistent with the `pg` library, in which the absence of a
    // TypeParser defaults to "noParse":
    // https://github.com/brianc/node-pg-types/blob/5b26b826466cff4a9092b8c9e31960fe293ef3d9/index.js#L15
    getTypeParser: typeOID => parsers[typeOID] ?? String,
  };
}
