import type {LogContext} from '@rocicorp/logger';
import {pgClient, type PostgresDB} from '../types/pg.js';

/**
 * Manages on-demand short-lived connections to a DB, shutting down after
 * an idle delay.
 */
export class ShortLivedClient {
  readonly #lc: LogContext;
  readonly #dbConnStr: string;
  readonly #appName: string;
  readonly #idleDelay: number;

  #db: PostgresDB | null = null;
  #dbTimeout: IdleTimeout = {};

  constructor(
    lc: LogContext,
    dbConnStr: string,
    appName: string,
    idleDelay = 10_000,
  ) {
    this.#lc = lc;
    this.#dbConnStr = dbConnStr;
    this.#appName = appName;
    this.#idleDelay = idleDelay;
  }

  /**
   * Get (or refresh) an upstream DB client that automatically closes
   * after being idle for 10 seconds.
   */
  get db(): PostgresDB {
    try {
      if (this.#db) {
        clearTimeout(this.#dbTimeout.id); // reset in finally
      } else {
        this.#db = pgClient(this.#lc, this.#dbConnStr, {
          connection: {['application_name']: this.#appName},
        });
      }
      return this.#db;
    } finally {
      const timeout = {
        id: setTimeout(() => {
          // Only close the connection if the #dbTimeout has not been reset.
          if (this.#dbTimeout === timeout) {
            this.#lc.debug?.(`closing idle upstream connection`);
            void this.#db?.end();
            this.#db = null;
          }
        }, this.#idleDelay),
      };
      this.#dbTimeout = timeout;
    }
  }
}

type IdleTimeout = {id?: NodeJS.Timeout};
