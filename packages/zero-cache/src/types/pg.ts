import pg from 'pg';
import postgres from 'postgres';
import array from 'postgres-array';
import {BigIntJSON} from './bigint-json.js';

const {
  types: {builtins, setTypeParser},
} = pg;

/** Registers types for the 'pg' library used by `pg-logical-replication`. */
export function registerPostgresTypeParsers() {
  setTypeParser(builtins.INT8, val => BigInt(val));
  setTypeParser(1016, val => array.parse(val, val => BigInt(val)));
}

/** Configures types for the Postgres.js client library (`postgres`). */
export const postgresTypeConfig = () => ({
  types: {
    bigint: postgres.BigInt,
    json: {
      to: 114, // builtins.JSON
      from: [114, 3802], // [builtins.JSON, builtins.JSONB]
      serialize: BigIntJSON.stringify,
      parse: BigIntJSON.parse,
    },
  },
});
