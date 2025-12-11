import type {LogContext} from '@rocicorp/logger';
import {setActiveUsersGetter} from '../../server/anonymous-otel-start.ts';
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

  // latest computed value exposed via the observable gauge callback
  #value = 0;
  #getterSet = false;

  constructor(
    lc: LogContext,
    db: PostgresDB,
    shard: ShardID,
    opts: Options = {},
  ) {
    this.#lc = lc;
    this.#db = db;
    this.#schema = cvrSchema(shard);
    this.#updateIntervalMs = opts.updateIntervalMs ?? 60 * 1000; // default 1 minute
  }

  async run(): Promise<void> {
    while (this.#state.shouldRun()) {
      try {
        const since = Date.now() - DAY;
        const [{cnt}] = await this.#db<[{cnt: bigint}]>`
          SELECT COUNT(*) AS cnt
          FROM ${this.#db(this.#schema)}.instances
          WHERE "lastActive" > ${since}
        `;
        this.#value = Number(cnt);

        // Set the getter after the first value is computed
        if (!this.#getterSet) {
          setActiveUsersGetter(() => this.#value);
          this.#getterSet = true;
        }

        this.#lc.debug?.(`updated active-users gauge to ${this.#value}`);
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
