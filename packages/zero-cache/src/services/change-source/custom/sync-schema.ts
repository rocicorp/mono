import type {LogContext} from '@rocicorp/logger';
import type {ShardConfig} from '../../../types/shards.ts';
import {initReplica} from '../common/replica-schema.ts';
import {initialSync} from './change-source.ts';

export async function initSyncSchema(
  log: LogContext,
  debugName: string,
  shard: ShardConfig,
  dbPath: string,
  upstreamURI: string,
): Promise<void> {
  await initReplica(log, debugName, dbPath, (log, tx) =>
    initialSync(log, shard, tx, upstreamURI),
  );
}
