import {
  PG_ADMIN_SHUTDOWN,
  PG_OBJECT_IN_USE,
  PG_OBJECT_NOT_IN_PREREQUISITE_STATE,
} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {defu} from 'defu';
import postgres, {type Options, type PostgresType} from 'postgres';
import {sleep} from '../../../../../../shared/src/sleep.ts';
import {getTypeParsers} from '../../../../db/pg-type-parser.ts';
import {type PostgresDB} from '../../../../types/pg.ts';
import {id, lit} from '../../../../types/sql.ts';
import {pipe, type Sink, type Source} from '../../../../types/streams.ts';
import {Subscription} from '../../../../types/subscription.ts';
import {AutoResetSignal} from '../../../change-streamer/schema/tables.ts';
import {fromBigInt} from '../lsn.ts';
import {PgoutputParser} from './pgoutput-parser.ts';
import type {Message} from './pgoutput.types.ts';

const DEFAULT_RETRIES_IF_REPLICATION_SLOT_ACTIVE = 5;

export type StreamMessage = [lsn: bigint, Message | {tag: 'keepalive'}];

export async function subscribe(
  lc: LogContext,
  db: PostgresDB,
  slot: string,
  publications: string[],
  lsn: bigint,
  retriesIfReplicationSlotActive = DEFAULT_RETRIES_IF_REPLICATION_SLOT_ACTIVE,
  applicationName = 'zero-replicator',
): Promise<{messages: Source<StreamMessage>; acks: Sink<bigint>}> {
  const session = postgres(
    defu(
      {
        max: 1,
        ['fetch_types']: false, // Necessary for the streaming protocol
        ['idle_timeout']: null,
        ['max_lifetime']: null as unknown as number,
        connection: {
          ['application_name']: applicationName,
          replication: 'database', // https://www.postgresql.org/docs/current/protocol-replication.html
        },
      },
      // ParsedOptions are technically compatible with Options, but happen
      // to not be typed that way. The postgres.js author does an equivalent
      // merge of ParsedOptions and Options here:
      // https://github.com/porsager/postgres/blob/089214e85c23c90cf142d47fb30bd03f42874984/src/subscribe.js#L13
      db.options as unknown as Options<Record<string, PostgresType>>,
    ),
  );

  // Postgres will send keepalives before timing out a wal_sender. It is possible that
  // these keepalives are not received if there is back-pressure in the replication
  // stream. To keep the connection alive, explicitly send keepalives if none have been
  // sent within the last 75% of the wal_sender_timeout.
  //
  // https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-WAL-SENDER-TIMEOUT
  const [{walSenderTimeoutMs}] = await session<
    {walSenderTimeoutMs: number}[]
  >`SELECT EXTRACT(EPOCH FROM (setting || unit)::interval) * 1000 
        AS "walSenderTimeoutMs" FROM pg_settings
        WHERE name = 'wal_sender_timeout'`.simple();
  const manualKeepaliveTimeout = Math.floor(walSenderTimeoutMs * 0.75);
  lc.info?.(
    `wal_sender_timeout: ${walSenderTimeoutMs}ms. ` +
      `Ensuring manual keepalives at least every ${manualKeepaliveTimeout}ms`,
  );

  const [readable, writable] = await startReplicationStream(
    lc,
    session,
    slot,
    publications,
    lsn,
    retriesIfReplicationSlotActive + 1,
  );

  let lastAckTime = Date.now();
  function sendAck(lsn: bigint) {
    writable.write(makeAck(lsn));
    lastAckTime = Date.now();
  }

  const livenessTimer = setInterval(() => {
    const now = Date.now();
    if (now - lastAckTime > manualKeepaliveTimeout) {
      sendAck(0n);
      lc.debug?.(`sent manual keepalive`);
    }
  }, manualKeepaliveTimeout / 5);

  let destroyed = false;
  const typeParsers = await getTypeParsers(db);
  const parser = new PgoutputParser(typeParsers);
  const messages = Subscription.create<StreamMessage>({
    cleanup: () => {
      destroyed = true;
      readable.destroyed || readable.destroy();
      clearInterval(livenessTimer);
      return session.end();
    },
  });

  readable.once(
    'close',
    () =>
      // Only log a warning if the stream was not manually closed.
      destroyed || lc.warn?.(`replication stream closed by ${db.options.host}`),
  );
  readable.once(
    'error',
    e =>
      // Don't log the shutdown signal. This is the expected way for upstream
      // to close the connection (and will be logged downstream).
      (e instanceof postgres.PostgresError && e.code === PG_ADMIN_SHUTDOWN) ||
      lc.error?.(`error from ${db.options.host}`, e),
  );

  pipe(readable, messages, buffer => parseStreamMessage(lc, buffer, parser));

  return {
    messages,
    acks: {push: sendAck},
  };
}

/**
 * Formats publication names for the START_REPLICATION command.
 * The replication protocol expects format: publication_names 'pub1,pub2'
 * Each name is escaped to handle any quotes that may have passed validation.
 */
function formatPublicationNames(publications: string[]): string {
  // lit() returns 'escaped_name' with surrounding quotes
  // We strip the quotes since the outer quotes are in the template
  return publications.map(p => lit(p).slice(1, -1)).join(',');
}

async function startReplicationStream(
  lc: LogContext,
  session: postgres.Sql,
  slot: string,
  publications: string[],
  lsn: bigint,
  maxAttempts: number,
) {
  for (let i = 0; i < maxAttempts; i++) {
    try {
      const stream = session
        .unsafe(
          `START_REPLICATION SLOT ${id(slot)} LOGICAL ${fromBigInt(lsn)} (
        proto_version '1',
        publication_names '${formatPublicationNames(publications)}',
        messages 'true'
      )`,
        )
        .execute();
      return await Promise.all([stream.readable(), stream.writable()]);
    } catch (e) {
      if (e instanceof postgres.PostgresError) {
        // error: replication slot "zero_slot_change_source_test_id" is active for PID 268
        if (e.code === PG_OBJECT_IN_USE) {
          // The freeing up of the replication slot is not transactional;
          // sometimes it takes time for Postgres to consider the slot
          // inactive.
          lc.warn?.(`attempt ${i + 1}: ${String(e)}`, e);
          await sleep(10);
          continue;
        }
        // error: This slot has been invalidated because it exceeded the maximum reserved size.
        // (This is a different manifestation of a slot being invalidated when
        //  the wal exceeds the max_slot_wal_keep_size)
        if (e.code === PG_OBJECT_NOT_IN_PREREQUISITE_STATE) {
          lc.error?.(`error starting replication stream`, e);
          throw new AutoResetSignal(`unable to start replication stream`, {
            cause: e,
          });
        }
      }
      throw e;
    }
  }
  throw new Error(
    `exceeded max attempts (${maxAttempts}) to start the Postgres stream`,
  );
}

function parseStreamMessage(
  lc: LogContext,
  buffer: Buffer,
  parser: PgoutputParser,
): StreamMessage | null {
  // https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA
  if (buffer[0] !== 0x77 && buffer[0] !== 0x6b) {
    lc.warn?.('Unknown message', buffer[0]);
    return null;
  }
  const lsn = buffer.readBigUInt64BE(1);
  if (buffer[0] === 0x77) {
    // XLogData
    return [lsn, parser.parse(buffer.subarray(25))];
  }
  if (buffer.readInt8(17)) {
    // Primary keepalive message: shouldRespond
    return [lsn, {tag: 'keepalive'}];
  }
  return null;
}

// https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-STANDBY-STATUS-UPDATE
function makeAck(lsn: bigint): Buffer {
  const microNow = BigInt(Date.now() - Date.UTC(2000, 0, 1)) * BigInt(1000);

  const x = Buffer.alloc(34);
  x[0] = 'r'.charCodeAt(0);
  x.writeBigInt64BE(lsn, 1);
  x.writeBigInt64BE(lsn, 9);
  x.writeBigInt64BE(lsn, 17);
  x.writeBigInt64BE(microNow, 25);
  return x;
}
