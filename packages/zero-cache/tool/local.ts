/**
 * Script to run replication logical locally. Run with `npm run local`,
 * optionally with Postgres Environment Variables to configure non-default
 * database connection values.
 *
 * https://www.postgresql.org/docs/current/libpq-envars.html
 *
 * Example:
 *
 * ```
 * $ PGPORT=5434 PGDATABASE=upstream npm run local
 * ```
 */

import '../../shared/src/dotenv.ts';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import {subscribe} from '../src/services/change-source/pg/logical-replication/stream.ts';
import {stringify} from '../src/types/bigint-json.ts';
import type {PostgresDB} from '../src/types/pg.ts';

const slotName = 'zero_slot';
const publicationNames = ['zero_data', 'zero_metadata'];

const lc = new LogContext('debug', {}, consoleLogSink);
const db = postgres() as PostgresDB;
const {messages, acks} = await subscribe(
  lc,
  db,
  slotName,
  publicationNames,
  0n,
);

for await (const [lsn, msg] of messages) {
  if (msg.tag === 'keepalive') {
    acks.push(0n);
  } else {
    lc.info?.(`"${lsn}": ${stringify(msg, null, 2)}`);
  }
}
