import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../../shared/src/must.ts';
import {mapValues} from '../../../../shared/src/objects.ts';
import {
  setActiveUsersGetter,
  type ActiveUsers,
} from '../../server/anonymous-otel-start.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cvrSchema, type ShardID} from '../../types/shards.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';

const MINUTE = 60 * 1000;
const DAY = 24 * 60 * MINUTE;

type Options = {
  updateIntervalMs?: number;
};

export class ActiveUsersGauge implements Service {
  readonly id = 'active-users-gauge';

  readonly #lc: LogContext;
  readonly #db: PostgresDB;
  readonly #schema: string;
  readonly #updateIntervalMs: number;
  readonly #state = new RunningState('active-users-gauge');
  readonly #setActiveUsersGetter: typeof setActiveUsersGetter;

  // latest computed value exposed via the observable gauge callback
  #lastActiveUsers: ActiveUsers | undefined;

  constructor(
    lc: LogContext,
    db: PostgresDB,
    shard: ShardID,
    opts: Options = {},
    setActiveUsersGetterFn = setActiveUsersGetter,
  ) {
    this.#lc = lc;
    this.#db = db;
    this.#schema = cvrSchema(shard);
    this.#updateIntervalMs = opts.updateIntervalMs ?? 60 * 1000; // default 1 minute
    this.#setActiveUsersGetter = setActiveUsersGetterFn;
  }

  async run(): Promise<void> {
    while (this.#state.shouldRun()) {
      try {
        const now = Date.now();
        const since30day = now - DAY * 30;
        const since7day = now - DAY * 7;
        const since1day = now - DAY;

        // This query performs a single scan over the `profile_ids_last_active`
        // index to compute aggregated results for all of our active user
        // metric variants.
        //
        // The eventually-correct metrics are `users_#da` which count distinct
        // profileIDs produced by the zero-client (i.e. starting with 'p').
        // The `users_#da_legacy` metrics include back-filled profileIDs (which
        // start with `cg`) and will over-count users on apps using memstore.
        const [actives] = await this.#db<[ActiveUsers]> /*sql*/ `
          SELECT 
            COUNT(*) FILTER (WHERE "lastActive" >= ${since1day}) AS active_users_last_day,
            COUNT(DISTINCT("profileID")) FILTER (WHERE "lastActive" >= ${since1day} AND starts_with("profileID", 'p')) AS users_1da,
            COUNT(DISTINCT("profileID")) FILTER (WHERE "lastActive" >= ${since7day} AND starts_with("profileID", 'p')) AS users_7da,
            COUNT(DISTINCT("profileID")) FILTER (WHERE starts_with("profileID", 'p')) AS users_30da,
            COUNT(DISTINCT("profileID")) FILTER (WHERE "lastActive" >= ${since1day}) AS users_1da_legacy,
            COUNT(DISTINCT("profileID")) FILTER (WHERE "lastActive" >= ${since7day}) AS users_7da_legacy,
            COUNT(DISTINCT("profileID")) AS users_30da_legacy
            FROM ${this.#db(this.#schema)}.instances
            WHERE "lastActive" >= ${since30day}
        `;
        // Determine if the getter needs to be set (i.e. the first time).
        const setGetter = this.#lastActiveUsers === undefined;
        this.#lastActiveUsers = mapValues(actives, bigVal => Number(bigVal));

        // Set the getter after the first value is computed
        if (setGetter) {
          this.#setActiveUsersGetter(() => must(this.#lastActiveUsers));
        }

        this.#lc.debug?.(`updated active-users gauge`, this.#lastActiveUsers);
      } catch (e) {
        this.#lc.warn?.('error updating active-users gauge', e);
      }

      await this.#state.sleep(this.#updateIntervalMs);
    }
  }

  stop(): Promise<void> {
    this.#state.stop(this.#lc);
    return this.#state.stopped();
  }
}
