import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import type {Service} from '../service.js';
import {initSyncSchema} from './schema/sync-schema.js';

export class Replicator implements Service {
  readonly id = 'replicator';
  readonly #lc: LogContext;
  readonly #syncReplica: postgres.Sql;

  constructor(lc: LogContext, syncReplicaUri: string) {
    this.#lc = lc.withContext('component', 'Replicator');
    this.#syncReplica = postgres(syncReplicaUri, {
      transform: postgres.camel,
    });
  }

  async start() {
    await initSyncSchema(this.#lc, this.#syncReplica);
  }
  async stop() {}
}
