import type {LogContext} from '@rocicorp/logger';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cvrSchema, type ShardID} from '../../types/shards.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';

const MINUTE = 60 * 1000;
const MIN_PURGE_INTERVAL_MS = MINUTE;
const MAX_PURGE_INTERVAL_MS = 16 * MINUTE;
const DEFAULT_CVRS_PER_PURGE = 1000;

export class CVRPurger implements Service {
  readonly id = 'reaper';

  readonly #lc: LogContext;
  readonly #db: PostgresDB;
  readonly #schema: string;
  readonly #inactivityThresholdMs: number;
  readonly #state = new RunningState('reaper');

  constructor(
    lc: LogContext,
    db: PostgresDB,
    shard: ShardID,
    inactivityThresholdMs: number,
  ) {
    this.#lc = lc;
    this.#db = db;
    this.#schema = cvrSchema(shard);
    this.#inactivityThresholdMs = inactivityThresholdMs;
  }

  async run() {
    let purgeable: number | undefined;
    let maxCVRsPerPurge = DEFAULT_CVRS_PER_PURGE;
    let purgeInterval = MIN_PURGE_INTERVAL_MS;

    while (this.#state.shouldRun()) {
      try {
        const {purged, remaining} =
          await this.purgeInactiveCVRs(maxCVRsPerPurge);

        if (purgeable !== undefined && remaining > purgeable) {
          // If the number of purgeable CVRs has grown even after the purge,
          // increase the number purged per round to achieve a steady state.
          maxCVRsPerPurge += DEFAULT_CVRS_PER_PURGE;
          this.#lc.info?.(`increased CVRs per purge to ${maxCVRsPerPurge}`);
        }
        purgeable = remaining;

        purgeInterval =
          purgeable > 0
            ? MIN_PURGE_INTERVAL_MS
            : Math.min(purgeInterval * 2, MAX_PURGE_INTERVAL_MS);
        this.#lc.info?.(
          `purged ${purged} inactive CVRs. Next purge in ${purgeInterval} ms`,
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
    return this.#db.begin(async sql => {
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
        // Rows only need to be deleted from the "instances" and "rowsVersion" tables.
        // Deletes will cascade through the other tables via foreign key references.
        for (const table of ['instances', 'rowsVersion']) {
          await sql`
            DELETE FROM ${sql(this.#schema)}.${sql(table)} WHERE "clientGroupID" IN ${sql(ids)}`;
        }
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
