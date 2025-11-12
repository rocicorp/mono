import type {LogContext} from '@rocicorp/logger';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {READ_COMMITTED} from '../../db/mode-enum.ts';
import {disableStatementTimeout, type PostgresDB} from '../../types/pg.ts';
import {cvrSchema, type ShardID} from '../../types/shards.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';

const MINUTE = 60 * 1000;
const MAX_PURGE_INTERVAL_MS = 16 * MINUTE;

type Options = {
  inactivityThresholdMs: number;
  initialBatchSize: number;
  initialIntervalMs: number;
};

export class CVRPurger implements Service {
  readonly id = 'reaper';

  readonly #lc: LogContext;
  readonly #db: PostgresDB;
  readonly #schema: string;
  readonly #inactivityThresholdMs: number;
  readonly #initialBatchSize: number;
  readonly #initialIntervalMs: number;
  readonly #state = new RunningState('reaper');

  constructor(
    lc: LogContext,
    db: PostgresDB,
    shard: ShardID,
    {inactivityThresholdMs, initialBatchSize, initialIntervalMs}: Options,
  ) {
    this.#lc = lc;
    this.#db = db;
    this.#schema = cvrSchema(shard);
    this.#inactivityThresholdMs = inactivityThresholdMs;
    this.#initialBatchSize = initialBatchSize;
    this.#initialIntervalMs = initialIntervalMs;
  }

  async run() {
    let purgeable: number | undefined;
    let maxCVRsPerPurge = this.#initialBatchSize;
    let purgeInterval = this.#initialIntervalMs;

    if (this.#initialBatchSize === 0) {
      this.#lc.warn?.(
        `CVR garbage collection is disabled (initialBatchSize = 0)`,
      );
      // Do nothing and just wait to be stopped.
      await this.#state.stopped();
    } else {
      this.#lc.info?.(
        `running cvr-purger with`,
        await this.#db`SHOW statement_timeout`,
      );
    }

    while (this.#state.shouldRun()) {
      try {
        const start = performance.now();
        const {purged, remaining} =
          await this.purgeInactiveCVRs(maxCVRsPerPurge);

        if (purgeable !== undefined && remaining > purgeable) {
          // If the number of purgeable CVRs has grown even after the purge,
          // increase the number purged per round to achieve a steady state.
          maxCVRsPerPurge += this.#initialBatchSize;
          this.#lc.info?.(`increased CVRs per purge to ${maxCVRsPerPurge}`);
        }
        purgeable = remaining;

        purgeInterval =
          purgeable > 0
            ? this.#initialIntervalMs
            : Math.min(purgeInterval * 2, MAX_PURGE_INTERVAL_MS);
        const elapsed = performance.now() - start;
        this.#lc.info?.(
          `purged ${purged} inactive CVRs (${elapsed.toFixed(2)} ms). Next purge in ${purgeInterval} ms`,
        );
        await this.#state.sleep(purgeInterval);
      } catch (e) {
        this.#lc.warn?.(`error encountered while garbage collecting CVRs`, e);
      }
    }
  }

  // Exported for testing.
  purgeInactiveCVRs(
    maxCVRs: number,
  ): Promise<{purged: number; remaining: number}> {
    return this.#db.begin(READ_COMMITTED, async sql => {
      disableStatementTimeout(sql);

      const threshold = Date.now() - this.#inactivityThresholdMs;
      // Implementation note: `FOR UPDATE` will prevent a syncer from
      // concurrently updating the CVR, since the update also performs
      // a `SELECT ... FOR UPDATE`, instead causing that update to
      // fail, which will cause the client to create a new CVR.
      //
      // `SKIP LOCKED` will skip over CVRs that a syncer is already
      // in the process of updating. In this manner, an in-progress
      // update effectively excludes the CVR from the purge.
      const ids = (
        await sql<{clientGroupID: string}[]>`
          SELECT "clientGroupID" FROM ${sql(this.#schema)}.instances
            WHERE "lastActive" < ${threshold}
            ORDER BY "lastActive" ASC
            LIMIT ${maxCVRs}
            FOR UPDATE SKIP LOCKED
      `.values()
      ).flat();

      if (ids.length > 0) {
        // Explicitly delete rows from cvr tables from "bottom" up. Even
        // though all tables eventually reference a ("top") ancestor row in the
        // "instances" or "rowsVersion" tables, relying on foreign key
        // cascading deletes can be suboptimal when the foreign key is not a
        // prefix of the primary key (e.g. the "desires" foreign key reference
        // to the "queries" table is not a prefix of the "desires" primary
        // key).
        await Promise.all(
          [
            'desires',
            'queries',
            'clients',
            'instances',
            'rows',
            'rowsVersion',
          ].map(table =>
            sql`
            DELETE FROM ${sql(this.#schema)}.${sql(table)} 
              WHERE "clientGroupID" IN ${sql(ids)}`.execute(),
          ),
        );
      }

      const [{remaining}] = await sql<[{remaining: bigint}]>`
        SELECT COUNT(*) AS remaining FROM ${sql(this.#schema)}.instances
          WHERE "lastActive" < ${threshold}
      `;

      return {purged: ids.length, remaining: Number(remaining)};
    });
  }

  stop(): Promise<void> {
    this.#state.stop(this.#lc);
    return promiseVoid;
  }
}
