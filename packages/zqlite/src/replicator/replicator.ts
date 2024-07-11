import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import fs from 'fs';
import type {LogContext} from '@rocicorp/logger';
import {PUBLICATION_NAME, SLOT_NAME} from '../consts.js';
import {copy} from './initial-sync.js';
import {MessageProcessor} from './message-processor.js';
import {DB, queries} from '../internal/db.js';

/**
 * The replicator attaches to the Postgres replication slot and listens for changes.
 *
 * Those changes are applied to the SQLite DB in a 1:1 mapping. 1 PG transaction -> 1 SQLite transaction.
 * 1 PG write -> 1 SQLite write.
 *
 * The changes are also accumulated, formatted into difference events, and pushed down the IVM pipelines.
 */
export class Replicator {
  readonly #pgConnectionString: string;
  readonly #sqliteDbPath: string;
  #replicationService: LogicalReplicationService | undefined;

  constructor(pgConnectionString: string, sqliteDbPath: string) {
    this.#pgConnectionString = pgConnectionString;
    this.#sqliteDbPath = sqliteDbPath;
  }

  async start(lc: LogContext) {
    try {
      await fs.promises.access(this.#sqliteDbPath);
    } catch (e) {
      lc.info?.('Starting initial sync to SQLite');
      await copy(lc, this.#pgConnectionString, this.#sqliteDbPath);
    }

    const db = new DB(this.#sqliteDbPath);
    const lastLsn =
      db.prepare(queries.getCommittedLsn).pluck().get() ?? '0/00000000';
    lc.debug?.('Last LSN:', lastLsn);

    const replicationService = (this.#replicationService =
      new LogicalReplicationService(
        {connectionString: this.#pgConnectionString},
        {acknowledge: {auto: false, timeoutSeconds: 0}},
      ));

    const messageProcessor = new MessageProcessor(this.#sqliteDbPath);
    this.#replicationService.on(
      'data',
      (lsn: string, message: Pgoutput.Message) => {
        lc.debug?.('DATA:', lsn, message);
        messageProcessor.processMessage(lc, lsn, message);
      },
    );
    this.#replicationService.on(
      'heartbeat',
      (_lsn: string, _time: number, shouldRespond: boolean) => {
        if (shouldRespond) {
          void replicationService.acknowledge(lastLsn);
        }
      },
    );

    await this.#replicationService.subscribe(
      new PgoutputPlugin({
        protoVersion: 1,
        publicationNames: [PUBLICATION_NAME],
      }),
      SLOT_NAME,
      lastLsn,
    );
    lc.info?.('Subscribed to Postgres changes');
  }
}
