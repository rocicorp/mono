/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import type {LogContext} from '@rocicorp/logger';
import type {ShardConfig} from '../../../types/shards.ts';
import {initReplica} from '../replica-schema.ts';
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
