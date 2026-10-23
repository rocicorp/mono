import type {LogContext} from '@rocicorp/logger';
import * as v from '../../../../../shared/src/valita.ts';
import {PG_17} from '../../../types/pg-versions.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {ShardID} from '../../../types/shards.ts';
import {toStateVersionString} from './lsn.ts';

/**
 * A fork request is a (non-transactional) logical message that a new
 * replication-manager emits once it has created the replication slot of a
 * replica that it is about to fork from a running replica, i.e. restore from
 * its backup.
 *
 * The fork waits for the running replica to have backed up (and acked) its
 * replication slot past the new slot's `consistentPoint`. A running
 * replication-manager otherwise backs up at its regular (e.g. 15 second)
 * cadence, which would delay the fork by as much. Upon receiving a fork
 * request in its replication stream, it instead backs up through the
 * `consistentPoint` as soon as its replica reaches it (see
 * `syncBackupThrough` in the change source protocol).
 *
 * The message is non-transactional so that it is streamed, and handled, as
 * soon as it is decoded, ahead of the transaction that inserts the new
 * replica's row (which reaches the running replica past the
 * `consistentPoint`). Replication-managers that do not recognize the prefix
 * ignore it, as they do any non-transactional message of another prefix.
 */
const forkRequestMessageSchema = v.object({
  replicaID: v.string(),
  slot: v.string(),
  consistentPoint: v.string(),
});

/** A fork request as it is emitted (see {@link emitForkRequest}). */
export type ForkRequest = v.Infer<typeof forkRequestMessageSchema>;

/**
 * Parses a {@link ForkRequest}, converting its `consistentPoint` (an LSN) to
 * the watermark through which to back up (`syncBackupThrough`). A malformed
 * LSN fails the parse, like any other malformed request, so that the request
 * is ignored rather than failing the replication stream.
 */
export const forkRequestSchema = forkRequestMessageSchema.chain(request => {
  try {
    return v.ok({
      ...request,
      syncBackupThrough: toStateVersionString(request.consistentPoint),
    });
  } catch (e) {
    return v.err(
      `malformed consistentPoint "${request.consistentPoint}": ${String(e)}`,
    );
  }
});

export type ParsedForkRequest = v.Infer<typeof forkRequestSchema>;

export function forkRequestMessagePrefix({appID, shardNum}: ShardID) {
  return `${appID}/${shardNum}/fork/v1`;
}

/**
 * Emits a {@link ForkRequest}. This only expedites the fork, which is correct
 * without it, so failures (e.g. a missing EXECUTE privilege on
 * `pg_logical_emit_message`) are logged rather than thrown.
 *
 * The message is flushed on PG 17+. Earlier versions lack the `flush`
 * parameter, in which case the subsequent commit of the new replica's row
 * flushes it.
 */
export async function emitForkRequest(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardID,
  request: ForkRequest,
): Promise<void> {
  const prefix = forkRequestMessagePrefix(shard);
  const content = JSON.stringify(request);
  try {
    const [{pgVersion}] = await sql<{pgVersion: number}[]>`
      SELECT current_setting('server_version_num')::int AS "pgVersion"`;
    if (pgVersion >= PG_17) {
      await sql`SELECT pg_logical_emit_message(
        false, ${prefix}::text, ${content}::text, true)`;
    } else {
      await sql`SELECT pg_logical_emit_message(
        false, ${prefix}::text, ${content}::text)`;
    }
    lc.info?.(`requested fork at ${request.consistentPoint}`, request);
  } catch (e) {
    lc.warn?.(
      `unable to emit fork request. The fork waits for the next regular ` +
        `backup of the replica it forks.`,
      e,
    );
  }
}
