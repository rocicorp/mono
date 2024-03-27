import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import {postgresTypeConfig} from '../../types/pg.js';
import type {Service} from '../service.js';
import {initSyncSchema} from './schema/sync-schema.js';

export class Replicator implements Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #syncReplica: postgres.Sql;

  constructor(
    lc: LogContext,
    replicaID: string,
    upstreamUri: string,
    syncReplicaUri: string,
  ) {
    this.id = replicaID;
    this.#lc = lc
      .withContext('component', 'Replicator')
      .withContext('serviceID', this.id);
    this.#upstreamUri = upstreamUri;
    this.#syncReplica = postgres(syncReplicaUri, {
      ...postgresTypeConfig(),
    });
  }

  async start() {
    await initSyncSchema(
      this.#lc,
      this.id,
      this.#syncReplica,
      this.#upstreamUri,
    );
  }
  async stop() {}
}
