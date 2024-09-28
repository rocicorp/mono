import {OID} from '@postgresql-typed/oids';
import {LogContext} from '@rocicorp/logger';
import pg from 'pg';
import postgres, {type Notice, type PostgresType} from 'postgres';
import array from 'postgres-array';
import {BigIntJSON, type JSONValue} from './bigint-json.js';

const {
  types: {builtins, setTypeParser},
} = pg;

const builtinsINT8ARRAY = 1016; // No definition in builtins for int8[]

/** Registers types for the 'pg' library used by `pg-logical-replication`. */
export function registerPostgresTypeParsers() {
  setTypeParser(builtins.INT8, val => BigInt(val));
  setTypeParser(builtinsINT8ARRAY, val => array.parse(val, val => BigInt(val)));
}

// Type these as `number` so that Typescript doesn't complain about
// referencing external types during type inference.
const builtinsJSON: number = builtins.JSON;
const builtinsJSONB: number = builtins.JSONB;

/** Configures types for the Postgres.js client library (`postgres`). */
export const postgresTypeConfig = () => ({
  types: {
    bigint: postgres.BigInt,
    json: {
      to: builtinsJSON,
      from: [builtinsJSON, builtinsJSONB],
      serialize: BigIntJSON.stringify,
      parse: BigIntJSON.parse,
    },
  },
});

export type PostgresDB = postgres.Sql<{
  bigint: bigint;
  json: JSONValue;
}>;

export type PostgresTransaction = postgres.TransactionSql<{
  bigint: bigint;
  json: JSONValue;
}>;

export function pgClient(
  lc: LogContext,
  connectionURI: string,
  options?: postgres.Options<{
    bigint: PostgresType<bigint>;
    json: PostgresType<JSONValue>;
  }>,
): PostgresDB {
  const onnotice = (n: Notice) => {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-RAISE
    switch (n.severity) {
      case 'NOTICE':
        return; // silenced
      case 'DEBUG':
        lc.debug?.(n);
        return;
      case 'WARNING':
      case 'EXCEPTION':
        lc.error?.(n);
        return;
      case 'LOG':
      case 'INFO':
      default:
        lc.info?.(n);
    }
  };
  return postgres(connectionURI, {
    ...postgresTypeConfig(),
    onnotice,
    ...options,
  });
}

export const typeNameByOID: Record<number, string> = Object.fromEntries(
  Object.entries(OID).map(([name, oid]) => [
    oid,
    name.startsWith('_') ? `${name.substring(1)}[]` : name,
  ]),
);

Object.freeze(typeNameByOID);
