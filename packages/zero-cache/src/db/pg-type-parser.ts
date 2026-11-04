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

// The only array type whose elements are not delimited by a comma.
const BOX_ARRAY_TYPE = 1020;

/**
 * Parses the text format of a Postgres array, e.g. `{1,NULL,"a b",{2,3}}`.
 *
 * This is used instead of the array parsers of postgres.js, which pass an
 * unquoted `NULL` to the element parser as if it were a value. Only a null
 * element is unquoted: Postgres quotes a string that spells `NULL`.
 *
 * postgres.js 4.0.0 fixes this with an equivalent parser, so this can be
 * removed when we upgrade to it. The 3.4.x releases don't have the fix.
 *
 * https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
 */
export function parseArray(
  text: string,
  parseElement: TypeParser = x => x,
  delimiter = ',',
): unknown[] {
  // An array with non-default bounds starts with them, e.g. `[2:3]={1,2}`.
  let pos = text.indexOf('{');
  assert(pos >= 0, 'array has no opening brace');

  function parseQuoted(): string {
    let value = '';
    for (pos++; text[pos] !== '"'; pos++) {
      assert(pos < text.length, 'unterminated quoted array element');
      if (text[pos] === '\\') {
        pos++;
      }
      value += text[pos];
    }
    pos++;
    return value;
  }

  function parseElements(): unknown[] {
    const elements: unknown[] = [];
    pos++;
    while (text[pos] !== '}') {
      assert(pos < text.length, 'unterminated array');
      if (text[pos] === '{') {
        elements.push(parseElements());
      } else if (text[pos] === '"') {
        elements.push(parseElement(parseQuoted()));
      } else {
        const start = pos;
        while (
          pos < text.length &&
          text[pos] !== delimiter &&
          text[pos] !== '}'
        ) {
          pos++;
        }
        const element = text.slice(start, pos);
        elements.push(element === 'NULL' ? null : parseElement(element));
      }
      if (text[pos] === delimiter) {
        pos++;
      }
    }
    pos++;
    return elements;
  }

  return parseElements();
}

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
  // postgres.js records the array type of each element type that it fetched.
  const {typeArrayMap} = (
    db.options as unknown as {shared: {typeArrayMap: Record<string, number>}}
  ).shared;
  const elementTypes = new Map(
    Object.entries(typeArrayMap).map(([element, array]) => [
      String(array),
      Number(element),
    ]),
  );
  const parsers = mapEntries(db.options.parsers, (type, parse) => {
    if ((type === JSON_TYPE || type === JSONB_TYPE) && returnJsonAsString) {
      return [type, (x: string) => x];
    }
    // The postgres.js library tags parsers for array types with an `array: true` field.
    // https://github.com/porsager/postgres/blob/089214e85c23c90cf142d47fb30bd03f42874984/src/connection.js#L760
    if (!(parse as unknown as {array?: boolean}).array) {
      return [type, parse];
    }
    const element = elementTypes.get(type);
    assert(element !== undefined, `no element type for array type ${type}`);
    const parseElement = db.options.parsers[element];
    const delimiter = Number(type) === BOX_ARRAY_TYPE ? ';' : ',';
    return [type, (val: string) => parseArray(val, parseElement, delimiter)];
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
